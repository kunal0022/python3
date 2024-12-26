import sqlalchemy
import pandas as pd
from datetime import datetime

# Create SQLAlchemy engines for Prod, Dev, and QA databases
engine_prod = sqlalchemy.create_engine('postgresql://chatjanssen_prod_admin:EvNcyrMw4=3t@itx-acm-genai-llm-prod-postgres01.czijpxum5el7.us-east-1.rds.amazonaws.com:5432/chat_prod')
engine_dev = sqlalchemy.create_engine('postgresql://chatjanssen_qa_admin:b5zHq!qntt=R@itx-acm-genai-llm-qa-postgres01.czijpxum5el7.us-east-1.rds.amazonaws.com:5432/chat_qa')
engine_qa = sqlalchemy.create_engine('postgresql://chatjanssen_dev_admin:o8lqgE#8Yo~T@itx-acm-genai-llm-dev-postgres01.czijpxum5el7.us-east-1.rds.amazonaws.com:5432/chat_dev')

current_date = datetime.now().strftime("%d%m%Y")
file_name = f"chat_services_{current_date}.xlsx"

# SQL query
query = """SELECT id, name, description, endpoint, chat_config_schema, message_config_schema, 
           CAST(updated_at AS TEXT), CAST(created_at AS TEXT), user_id, "order", published, slug, background_image_key 
           FROM public.chat_services"""

# Engines and corresponding sheet names
databases = [
    (engine_prod, "prod"),
    (engine_dev, "dev"),
    (engine_qa, "qa")
]

try:
    # Execute query for each database and save results to Excel in different sheets
    with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
        for engine, sheet_name in databases:
            df = pd.read_sql(query, engine)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

except Exception as e:
    print(f"Error in executing SQL: {e}")

# Dispose of the SQLAlchemy engines
engine_prod.dispose()
engine_dev.dispose()
engine_qa.dispose()
