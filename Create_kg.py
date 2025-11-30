import pandas as pd
from neo4j import GraphDatabase

def read_config(path="config.txt"):
    cfg = {}
    with open(path, "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                cfg[k.strip()] = v.strip()
    return cfg["URI"], cfg["USERNAME"], cfg["PASSWORD"]

def to_bool(x):
    """Convert Yes/No, True/False, 1/0 to boolean."""
    if pd.isna(x):
        return False
    s = str(x).strip().lower()
    return s in ["yes", "true", "1", "y"]

# Neo4j 
def create_constraints(tx):
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Traveller) REQUIRE t.user_id IS UNIQUE")
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (h:Hotel) REQUIRE h.hotel_id IS UNIQUE")
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE")
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:City) REQUIRE c.name IS UNIQUE")
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (co:Country) REQUIRE co.name IS UNIQUE")

def load_travellers(tx, users_df):
    # users.csv columns:
    # user_id, user_gender, country, age_group, traveller_type, join_date
    query = """
    UNWIND $rows AS row
    MERGE (t:Traveller {user_id: row.user_id})
    SET t.age = row.age_group,
        t.type = row.traveller_type,
        t.gender = row.user_gender
    """
    tx.run(query, rows=users_df.to_dict("records"))

def load_hotels(tx, hotels_df):
    # hotels.csv columns:
    # hotel_id, hotel_name, city, country, star_rating, lat, lon,
    # cleanliness_base, comfort_base, facilities_base, location_base,
    # staff_base, value_for_money_base
    query = """
    UNWIND $rows AS row
    MERGE (h:Hotel {hotel_id: row.hotel_id})
    SET h.name = row.hotel_name,
        h.star_rating = row.star_rating,
        h.cleanliness_base = row.cleanliness_base,
        h.comfort_base = row.comfort_base,
        h.facilities_base = row.facilities_base
    """
    tx.run(query, rows=hotels_df.to_dict("records"))

def load_cities_countries(tx, hotels_df, users_df):
    # Create Country nodes from both hotels + users
    countries = pd.concat([hotels_df["country"], users_df["country"]]).dropna().unique().tolist()
    tx.run("""
    UNWIND $countries AS name
    MERGE (:Country {name: name})
    """, countries=countries)

    # Create City nodes from hotels
    cities = hotels_df["city"].dropna().unique().tolist()
    tx.run("""
    UNWIND $cities AS name
    MERGE (:City {name: name})
    """, cities=cities)

    # Hotel -> City
    tx.run("""
    UNWIND $rows AS row
    MATCH (h:Hotel {hotel_id: row.hotel_id})
    MATCH (c:City {name: row.city})
    MERGE (h)-[:LOCATED_IN]->(c)
    """, rows=hotels_df[["hotel_id", "city"]].to_dict("records"))

    # City -> Country
    tx.run("""
    UNWIND $rows AS row
    MATCH (c:City {name: row.city})
    MATCH (co:Country {name: row.country})
    MERGE (c)-[:LOCATED_IN]->(co)
    """, rows=hotels_df[["city", "country"]].to_dict("records"))

    # Traveller -> Country (FROM_COUNTRY)
    tx.run("""
    UNWIND $rows AS row
    MATCH (t:Traveller {user_id: row.user_id})
    MATCH (co:Country {name: row.country})
    MERGE (t)-[:FROM_COUNTRY]->(co)
    """, rows=users_df[["user_id", "country"]].to_dict("records"))

def load_reviews_and_edges(tx, reviews_df):
    # reviews.csv columns:
    # review_id, user_id, hotel_id, review_date, score_overall,
    # score_cleanliness, score_comfort, score_facilities, score_location,
    # score_staff, score_value_for_money, review_text

    # Review nodes
    tx.run("""
    UNWIND $rows AS row
    MERGE (r:Review {review_id: row.review_id})
    SET r.text = row.review_text,
        r.date = row.review_date,
        r.score_overall = row.score_overall,
        r.score_cleanliness = row.score_cleanliness,
        r.score_comfort = row.score_comfort,
        r.score_facilities = row.score_facilities,
        r.score_location = row.score_location,
        r.score_staff = row.score_staff,
        r.score_value_for_money = row.score_value_for_money
    """, rows=reviews_df.to_dict("records"))

    # Traveller WROTE Review
    tx.run("""
    UNWIND $rows AS row
    MATCH (t:Traveller {user_id: row.user_id})
    MATCH (r:Review {review_id: row.review_id})
    MERGE (t)-[:WROTE]->(r)
    """, rows=reviews_df[["user_id", "review_id"]].to_dict("records"))

    # Review REVIEWED Hotel
    tx.run("""
    UNWIND $rows AS row
    MATCH (r:Review {review_id: row.review_id})
    MATCH (h:Hotel {hotel_id: row.hotel_id})
    MERGE (r)-[:REVIEWED]->(h)
    """, rows=reviews_df[["review_id", "hotel_id"]].to_dict("records"))

    # Traveller STAYED_AT Hotel (derived from review)
    tx.run("""
    UNWIND $rows AS row
    MATCH (t:Traveller {user_id: row.user_id})
    MATCH (h:Hotel {hotel_id: row.hotel_id})
    MERGE (t)-[:STAYED_AT]->(h)
    """, rows=reviews_df[["user_id", "hotel_id"]].to_dict("records"))

def set_average_reviews_score(tx):
    tx.run("""
    MATCH (h:Hotel)<-[:REVIEWED]-(r:Review)
    WITH h, avg(r.score_overall) AS avgScore
    SET h.average_reviews_score = avgScore
    """)

def load_visa_edges(tx, visa_df):
    # visa.csv columns: from, to, requires_visa, visa_type
    visa_df = visa_df.copy()
    visa_df["requires_bool"] = visa_df["requires_visa"].apply(to_bool)

    tx.run("""
    UNWIND $rows AS row
    MATCH (c1:Country {name: row.from})
    MATCH (c2:Country {name: row.to})
    FOREACH (_ IN CASE WHEN row.requires_bool THEN [1] ELSE [] END |
        MERGE (c1)-[v:NEEDS_VISA]->(c2)
        SET v.visa_type = row.visa_type
    )
    """, rows=visa_df.to_dict("records"))


# Main
def main():
    uri, user, pwd = read_config()

    hotels_df  = pd.read_csv("hotels.csv")
    reviews_df = pd.read_csv("reviews.csv")
    users_df   = pd.read_csv("users.csv")
    visa_df    = pd.read_csv("visa.csv")

    driver = GraphDatabase.driver(uri, auth=(user, pwd))

    with driver.session() as session:
        session.execute_write(create_constraints)
        session.execute_write(load_travellers, users_df)
        session.execute_write(load_hotels, hotels_df)
        session.execute_write(load_cities_countries, hotels_df, users_df)
        session.execute_write(load_reviews_and_edges, reviews_df)
        session.execute_write(set_average_reviews_score)
        session.execute_write(load_visa_edges, visa_df)

    driver.close()
    print("KG created successfully.")

if __name__ == "__main__":
    main()
