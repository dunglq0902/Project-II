import argparse
import uuid
from datetime import datetime
from pymongo import MongoClient

def seed_data(mongo_uri, db_name):
    print(f"Connecting to MongoDB at {mongo_uri}...")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    
    collection = db['alert_rules']
    
    # Check if there are already rules
    if collection.count_documents({}) > 0:
        print("Alert rules already seeded. Skipping.")
        return

    # Sample alert rules
    rules = [
        {
            "rule_id": str(uuid.uuid4()),
            "user_id": "test_user_1",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "conditions": [
                {
                    "field": "rsi_14",
                    "operator": "<",
                    "value": 30.0
                }
            ],
            "logic": "AND",
            "action": "BUY",
            "notification_channels": ["email"],
            "email_address": "test@example.com",
            "cooldown_seconds": 300,
            "is_active": True,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "trigger_count": 0
        },
        {
            "rule_id": str(uuid.uuid4()),
            "user_id": "test_user_1",
            "symbol": "ETHUSDT",
            "timeframe": "1h",
            "conditions": [
                {
                    "field": "rsi_14",
                    "operator": ">",
                    "value": 70.0
                }
            ],
            "logic": "AND",
            "action": "SELL",
            "notification_channels": ["email"],
            "email_address": "test@example.com",
            "cooldown_seconds": 300,
            "is_active": True,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "trigger_count": 0
        }
    ]
    
    result = collection.insert_many(rules)
    print(f"Seeded {len(result.inserted_ids)} alert rules.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed MongoDB with sample alert rules.")
    parser.add_argument("--mongo-uri", required=True, help="MongoDB connection URI")
    parser.add_argument("--db", required=True, help="Database name")
    
    args = parser.parse_args()
    seed_data(args.mongo_uri, args.db)
