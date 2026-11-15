"""Executed inside the existing Superset container by publish_market_superset.

Create/update only the named Market database, datasets, charts and dashboard.
Verify each chart using Superset's actual chart-data HTTP endpoint.
"""
import json
import logging


def publish(reader_url, *, validate=True):
    logging.getLogger().setLevel(logging.ERROR)
    from superset.app import create_app
    app = create_app()
    app.config['WTF_CSRF_ENABLED'] = False  # Only this in-process validation client.
    with app.app_context():
        from superset import db, security_manager
        from superset.models.core import Database
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.slice import Slice
        from superset.models.dashboard import Dashboard
        admin = security_manager.find_user(username='admin')
        if admin is None:
            raise RuntimeError('Superset admin account not found')
        database = db.session.query(Database).filter_by(database_name='TopCV Market Warehouse').one_or_none()
        if database is None:
            database = Database(database_name='TopCV Market Warehouse', expose_in_sqllab=True,
                                allow_dml=False, allow_ctas=False, allow_cvas=False)
            db.session.add(database)
        database.set_sqlalchemy_uri(reader_url)
        db.session.flush()
        datasets = {}
        for name in ('collection_overview', 'coverage_freshness', 'observed_listings', 'published_jobs'):
            dataset = db.session.query(SqlaTable).filter_by(database_id=database.id,
                schema='market_dw', table_name=name).one_or_none()
            if dataset is None:
                dataset = SqlaTable(database=database, schema='market_dw', table_name=name,
                    owners=[admin], cache_timeout=0,
                    description='Mẫu tin TopCV đã quan sát; detail chỉ công bố sau DQ. Không suy ra tổng thị trường hay job đã đóng từ coverage thiếu.')
                db.session.add(dataset)
                db.session.flush()
            dataset.fetch_metadata()
            labels = {'job_title':'Tên tin tuyển dụng','position_name':'Vị trí chuẩn','position_status':'Phân loại',
                'company_name':'Doanh nghiệp','cities':'Thành phố','city':'Thành phố','locations':'Địa điểm',
                'salary_label':'Lương nguồn công bố','salary_status':'Tình trạng lương',
                'experience_bucket':'Kinh nghiệm','skills':'Kỹ năng','skill_name':'Kỹ năng chuẩn',
                'job_level':'Cấp bậc','company_size':'Quy mô công ty','industry':'Lĩnh vực',
                'posted_date_label':'Ngày đăng','deadline_date_label':'Hạn ứng tuyển','benefits':'Quyền lợi',
                'job_description':'Mô tả công việc','job_requirements':'Yêu cầu','cleaning_status':'Chất lượng làm sạch',
                'snapshot_date':'Ngày quan sát','canonical_job_url':'Tin gốc','job_count':'Số tin',
                'average_salary_label':'Lương trung bình','salary_sample_count':'Số mẫu lương',
                'salary_availability':'Mức độ có dữ liệu lương','status_label':'Trạng thái crawl',
                'attempts':'Lượt xử lý','failure_count':'Số lượt gặp lỗi','next_retry_at':'Thử lại lúc (giờ VN)','last_error':'Sự cố gần nhất',
                'is_temporal_eligible':'Đủ thời gian thu thập xác thực',
                'observed_jobs':'Tin listing đã quan sát', 'unique_observed_postings':'Tin có detail đã qua DQ',
                'posted_date_verified_count':'Tin có ngày đăng xác minh', 'salary_known_count':'Số quan sát biết lương',
                'listing_only_fraction':'Tỷ lệ listing chưa có detail', 'last_successful_refresh':'DW refresh thành công gần nhất',
                'quarantined_records':'Record đang quarantine', 'record_level':'Mức dữ liệu',
                'batch_date':'Ngày tạo batch', 'batch_state':'Trạng thái batch', 'expected_scopes':'Scope cấu hình',
                'attempted_scopes':'Scope đã thử', 'unattempted_scopes':'Scope chưa thử', 'complete_scopes':'Scope hoàn tất',
                'scopes_with_eligible_data':'Scope có dữ liệu dùng được', 'scopes_with_work_remaining':'Scope còn việc',
                'scopes_held':'Scope đang tạm dừng', 'coverage_complete':'Đã đủ phạm vi',
                'scope_first_attempt_coverage':'Tỷ lệ scope đã thử (%)', 'scope_data_coverage':'Tỷ lệ scope có dữ liệu (%)',
                'bi_freshness_minutes':'Phút từ DW refresh thành công', 'listing_captured_at':'Thời điểm chụp listing',
                'listing_only':'Chưa có detail', 'normalized_position':'Vị trí chuẩn', 'observed_lifecycle_status':'Vòng đời được xác minh'}
            for column in dataset.columns:
                if column.column_name in labels:
                    column.verbose_name = labels[column.column_name]
            datasets[name] = dataset
        db.session.flush()

        def metric(column, label=None, expression=None):
            return {'expressionType': 'SQL', 'sqlExpression': expression or 'SUM(' + column + ')',
                    'label': label or column, 'optionName': 'market_' + column}

        specs = [
            ('Tin listing đã quan sát', 'collection_overview', 'big_number_total', [], [metric('observed_jobs')]),
            ('Tin có detail đã qua DQ', 'collection_overview', 'big_number_total', [], [metric('unique_observed_postings')]),
            ('Mẫu lương VND/tháng đã biết', 'collection_overview', 'big_number_total', [], [metric('salary_known_count')]),
            ('Tin có ngày đăng xác minh', 'collection_overview', 'big_number_total', [], [metric('posted_date_verified_count')]),
            ('Tỷ lệ listing chưa có detail', 'collection_overview', 'big_number_total', [],
             [metric('listing_only_fraction', expression='AVG(listing_only_fraction)')]),
            ('Record đang quarantine', 'collection_overview', 'big_number_total', [], [metric('quarantined_records')]),
            ('Tin theo vị trí trong mẫu quan sát', 'observed_listings', 'echarts_timeseries_bar', ['normalized_position'],
             [metric('source_job_id', 'Tin đã quan sát', 'COUNT(DISTINCT source_job_id)')]),
            ('Tin theo thành phố trong mẫu quan sát', 'observed_listings', 'echarts_timeseries_bar', ['city'],
             [metric('source_job_id', 'Tin đã quan sát', 'COUNT(DISTINCT source_job_id)')]),
            ('Chất lượng detail đã phát hành', 'published_jobs', 'table', ['record_level','cleaning_status'],
             [metric('raw_job_id', 'Quan sát detail', 'COUNT(*)')]),
            ('Độ phủ thực tế của batch', 'coverage_freshness', 'table',
             ['batch_date','batch_state','expected_scopes','attempted_scopes','unattempted_scopes',
              'complete_scopes','scopes_with_eligible_data','scopes_with_work_remaining','scopes_held',
              'coverage_complete','scope_first_attempt_coverage','scope_data_coverage'], []),
            ('Độ mới dữ liệu đã xử lý', 'coverage_freshness', 'table',
             ['batch_date','latest_scope_capture','last_successful_refresh','bi_freshness_minutes'], []),
            ('Listing đã quan sát, gồm tin chưa có detail', 'observed_listings', 'table',
             ['job_title','normalized_position','company_name','city','salary_label','record_level',
              'listing_only','listing_captured_at','canonical_job_url'], []),
            ('Detail đã qua DQ trong mẫu quan sát', 'published_jobs', 'table',
             ['job_title','position_id','salary_label','salary_status','experience_bucket','posted_date_label',
              'deadline_date_label','cleaning_status','record_level','observed_lifecycle_status',
              'snapshot_date','canonical_job_url'], []),
        ]
        charts = []
        contexts = []
        for title, dataset_name, viz_type, columns, metrics in specs:
            dataset = datasets[dataset_name]
            name = 'Market | ' + title
            chart = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
            if chart is None:
                chart = Slice(slice_name=name, owners=[admin])
                db.session.add(chart)
            chart.slice_name = name
            form = {'datasource': str(dataset.id) + '__table', 'viz_type': viz_type,
                    'metrics': metrics, 'groupby': columns if metrics else [],
                    'all_columns': columns if not metrics else [], 'query_mode': 'aggregate' if metrics else 'raw',
                    'row_limit': 1000, 'time_range': 'No filter', 'adhoc_filters': [],
                    'show_legend': False, 'show_value': True, 'truncate_metric': True,
                    'x_axis': columns[0] if columns and metrics else None,
                    'x_axis_time_format': '%Y-%m-%d', 'number_format': ',.0f',
                    'page_length': 20, 'include_search': True, 'table_timestamp_format': '%Y-%m-%d'}
            if viz_type == 'big_number_total':
                form['metric'] = metrics[0]
                if dataset_name == 'collection_overview' and metrics[0]['label'] == 'listing_only_fraction':
                    form['number_format'] = '.1%'
            if viz_type == 'echarts_timeseries_bar':
                form['orientation'] = 'horizontal'
                form['order_desc'] = True
                form['x_axis_sort'] = metrics[0]['label']
                form['x_axis_sort_asc'] = False
            if dataset_name == 'top_skills':
                form['row_limit'] = 15
            query = {'columns': columns, 'metrics': metrics, 'row_limit': 1000,
                     'is_timeseries': False, 'filters': [], 'extras': {'having':'', 'where':''},
                     'annotation_layers': [], 'orderby': [], 'time_range': 'No filter'}
            context = {'datasource': {'id': dataset.id, 'type': 'table'}, 'queries': [query],
                       'form_data': form, 'result_format': 'json', 'result_type': 'full', 'force': True}
            query['row_limit'] = form['row_limit']
            if metrics:
                query['orderby'] = [[metrics[0],False]]
            chart.datasource_id, chart.datasource_type = dataset.id, 'table'
            chart.datasource_name, chart.viz_type = dataset.table_name, viz_type
            chart.params = json.dumps(form, ensure_ascii=False)
            chart.query_context = json.dumps(context, ensure_ascii=False)
            chart.cache_timeout = 0
            chart.description = ('Chỉ mô tả tập tin đã quan sát. Độ phủ thiếu không chứng minh job đã đóng hoặc nhu cầu toàn thị trường thay đổi. '
                                 'Detail dùng nguồn công bố qua DQ; giá trị thiếu giữ unknown/NULL và cờ hợp lệ.')
            charts.append(chart)
            contexts.append(context)
        db.session.flush()
        dashboard = db.session.query(Dashboard).filter_by(slug='topcv-market').one_or_none()
        if dashboard is None:
            dashboard = Dashboard(slug='topcv-market', owners=[admin])
            db.session.add(dashboard)
        dashboard.dashboard_title = 'TopCV · Mẫu tin IT đã quan sát · Coverage có thể thiếu'
        dashboard.description = ('Dữ liệu quan sát từng phần, không phải tổng việc làm toàn thị trường. '
            'Listing chưa có detail vẫn được thống kê riêng; detail chỉ công bố khi DQ pass. '
            'Theo dõi coverage, freshness, quarantine và nguồn bị chặn độc lập. '
            'Không suy ra job mới/đóng hoặc tăng giảm cầu thị trường khi coverage chưa đủ; lifecycle hiển thị UNKNOWN.')
        dashboard.published = True
        dashboard.slices = charts
        layout = {'DASHBOARD_VERSION_KEY':'v2',
            'ROOT_ID': {'id':'ROOT_ID','type':'ROOT','children':['GRID_ID']},
            'GRID_ID': {'id':'GRID_ID','type':'GRID','parents':['ROOT_ID'],'children':[]},
            'HEADER_ID': {'id':'HEADER_ID','type':'HEADER','meta':{'text':dashboard.dashboard_title}}}
        for index in range(0, len(charts), 2):
            row_id = 'ROW-' + str(index // 2)
            pair = charts[index:index+2]
            if index == len(charts) - 1:
                pair = [charts[index]]
            layout['GRID_ID']['children'].append(row_id)
            layout[row_id] = {'id':row_id,'type':'ROW','parents':['ROOT_ID','GRID_ID'],
                              'children':[], 'meta':{'background':'BACKGROUND_TRANSPARENT'}}
            for chart in pair:
                cid = 'CHART-' + str(chart.id)
                layout[row_id]['children'].append(cid)
                layout[cid] = {'id':cid,'type':'CHART','parents':['ROOT_ID','GRID_ID',row_id],
                    'children':[], 'meta':{'chartId':chart.id,'sliceName':chart.slice_name,
                    'uuid':str(chart.uuid),'width':12 if len(pair)==1 else 6,'height':65 if chart.viz_type=='table' else 45}}
        dashboard.position_json = json.dumps(layout, ensure_ascii=False)
        dashboard.json_metadata = json.dumps({'refresh_frequency':300,'color_scheme':'supersetColors',
            'cross_filters_enabled': True, 'expanded_slices':{}, 'timed_refresh_immune_slices':[]})
        db.session.commit()

        validations = []
        if validate:
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session['_user_id'] = str(admin.id)
                    session['_fresh'] = True
                for chart, context in zip(charts, contexts):
                    response = client.post('/api/v1/chart/data', json=context)
                    body = response.get_json() or {}
                    items = body.get('result') or []
                    errors = [item.get('error') for item in items if item.get('error')]
                    validations.append({'chart_id':chart.id,'title':chart.slice_name,'http_status':response.status_code,
                        'row_count':len(items[0].get('data',[])) if items else 0,
                        'passed':response.status_code==200 and bool(items) and not errors})
        result = {'dashboard_id':dashboard.id,'path':'/superset/dashboard/topcv-market/',
                  'dashboard_policy_version':'readme-partial-v1',
                  'configuration_published':True,'configured_chart_count':len(charts),
                  'validation_performed':validate,'charts':validations,
                  'all_charts_passed':all(v['passed'] for v in validations) if validate else None}
        print('MARKET_RESULT=' + json.dumps(result, ensure_ascii=False))

