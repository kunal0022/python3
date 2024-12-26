import sqlalchemy
import pandas as pd
from datetime import datetime

# Create a SQLAlchemy engine
#engine = sqlalchemy.create_engine('postgresql://chatuser:A!b2C#d4@ahjfpd01.postgres.database.azure.com:5432/chat')
engine = sqlalchemy.create_engine('postgresql://chatjanssen_prod_admin:EvNcyrMw4=3t@itx-acm-genai-llm-prod-postgres01.czijpxum5el7.us-east-1.rds.amazonaws.com:5432/chat_prod')
current_date = datetime.now().strftime("%d%m%Y")
sheet_name = f"chatjanssen_app_metrics_{current_date}"

# SQL queries
queries = [
    """SELECT cs.name, first_name, last_name, email 
       FROM chat_services cs 
       INNER JOIN chat_service_grants csg ON cs.id=csg.chat_service_id
       INNER JOIN users_groups ug ON ug.group_id=csg.group_id
       INNER JOIN users u ON u.id=ug.user_id
       --WHERE cs.name IN ('Ask MAIA','Chat Citeline','MAIA Chat','MAIA Chat IRA','QnA Chat') 
       ORDER BY cs.name, first_name, last_name""",
    """select cs.name app_name,count(email) number_of_users_having_access from chat_services cs inner join
chat_service_grants csg on cs.id=csg.chat_service_id
inner join users_groups ug on ug.group_id=csg.group_id
inner join users u on u.id=ug.user_id
--where cs.name in ('Ask MAIA','Chat Citeline','MAIA Chat','MAIA Chat IRA','QnA Chat')
group by cs.name""",
    """Select cs.name,first_name,last_name,email from chat_services cs inner join chats c
on cs.id=c.chat_service_id
inner join users u on u.id=c.user_id
--where cs.name in ('Ask MAIA','Chat Citeline','MAIA Chat','MAIA Chat IRA','QnA Chat')
group by cs.name,first_name,last_name,email
order by cs.name,first_name,last_name,email""",
    """Select cs.name,count(distinct email) from chat_services cs inner join chats c
on cs.id=c.chat_service_id
inner join users u on u.id=c.user_id
--where cs.name in ('Ask MAIA','Chat Citeline','MAIA Chat','MAIA Chat IRA','QnA Chat')
group by cs.name
order by cs.name""",
    """SELECT 
    cs.name,
    u.first_name,
    u.last_name,
    u.email,
    COUNT(m.content) AS message_count,
    cast(MAX(m.created_at) as TEXT) AS last_accessed
FROM 
    chats c
INNER JOIN 
    messages m ON m.chat_id = c.id
INNER JOIN 
    chat_services cs ON cs.id = c.chat_service_id
INNER JOIN 
    users u ON u.id = m.user_id
--WHERE 
--    cs.name IN ('Ask MAIA', 'Chat Citeline', 'MAIA Chat', 'MAIA Chat IRA', 'QnA Chat')
GROUP BY 
    cs.name, u.first_name, u.last_name, u.email
ORDER BY 
    cs.name,MAX(m.created_at) desc, u.first_name, u.last_name, u.email
""",
    """Select cs.name,first_name,last_name,email,--m.content,
fm.is_positive_feedback,fm.resp_accurate_and_consistent,fm.resp_concise_and_understandable,fm.resp_speed from flagged_messages fm
inner join messages m on fm.message_id=m.id
inner join chats c on c.id=m.chat_id
inner join chat_services cs on cs.id=c.chat_service_id
inner join users u on u.id=m.user_id
--where cs.name in ('Chat Janssen','Ask MAIA','Chat Citeline','MAIA Chat','MAIA Chat IRA','QnA Chat')
order by cs.name,first_name,last_name,email"""
]

# Sheet names for the Excel workbook
sheet_names = [
    "who_has_access", "how_many_have_access", "who_has_accessed_so_far", "how_many_accessed_so_far", "no_of_questions",
    "thumbs_up_feedback"  # Rename accordingly for each query
    # Add more sheet names accordingly...
]

try:
    # Execute each query and save results to Excel
    with pd.ExcelWriter(f'report_{sheet_name}.xlsx', engine='openpyxl') as writer:
        for query, sheet_name in zip(queries, sheet_names):
            df = pd.read_sql(query, engine)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

except Exception as e:
    print(f"Error in executing some SQLs : {e}")

# Dispose the SQLAlchemy engine
engine.dispose()

