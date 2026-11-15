"""The ten requested positions; discovery keywords never assign job roles."""

POSITIONS = {
    'software_engineer': 'Software Engineer',
    'backend_developer': 'Backend Developer',
    'data_analyst': 'Data Analyst',
    'fullstack_developer': 'Fullstack Developer',
    'ai_engineer': 'AI Engineer',
    'data_scientist': 'Data Scientist',
    'data_engineer': 'Data Engineer',
    'devops_engineer': 'DevOps Engineer',
    'cybersecurity_specialist': 'Cybersecurity Specialist',
    'business_analyst': 'Business Analyst',
}
POSITION_IDS = {name: key for key, name in POSITIONS.items()}
POSITION_IDS['Machine Learning Engineer'] = 'ai_engineer'
# Keep the canonical ID of historical Frontend postings after retiring its scopes.
POSITION_IDS['Frontend Developer'] = 'frontend_developer'
UNKNOWN = 'Chưa xác định'
UNDISCLOSED = 'Chưa công bố'
VERSION = 'market-cleaning-v2.3'
