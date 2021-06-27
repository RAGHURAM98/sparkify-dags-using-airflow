class DataQualitySqlQueries:
    
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE user_id is null", 'expected_result': 0, 'table':'users' },
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0, 'table':'time'},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id is null", 'expected_result': 0, 'table':'songs'},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE songplay_id is null", 'expected_result': 0, 'table':'songplays'},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artist_id is null", 'expected_result': 0, 'table':'artists'}
    ]
    
    