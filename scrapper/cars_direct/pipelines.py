# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter


# class CarsDirectPipeline:
#     def process_item(self, item, spider):
#         return item

import psycopg2
from psycopg2 import sql
from scrapy.exceptions import DropItem

class PostgresPipeline:
    def open_spider(self, spider):
        # Connect to PostgreSQL database
        self.conn = psycopg2.connect(
                host="localhost",
                database="cars_db",
                user="cars_user",
                password="small0world",
                port=5432
            )

        
        self.cursor = self.conn.cursor()
        print("Created db conn")

        # Dynamically create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS cars_data (
            detail_page_link TEXT,
            make TEXT,
            title TEXT,
            trim_name TEXT,
            car_images TEXT,
            exterior_color TEXT,
            interior_color TEXT,
            transmission TEXT,
            engine TEXT,
            certified_preowned TEXT,
            doors TEXT,
            stock_id TEXT,
            vin TEXT,
            list_price TEXT,
            loan_estimate TEXT,
            mileage TEXT,
            dealer_info TEXT,
            carfaxOwnerNumber TEXT,
            features TEXT,
            seller_comments TEXT,
            interest_rate TEXT,
            down_payment_percentage TEXT,
            down_payment_value TEXT,
            dealer_name TEXT,
            dealer_address TEXT,
            dealer_phone TEXT,
            dealer_website TEXT
        );
        """
        self.cursor.execute(create_table_query)
        self.conn.commit()
        print("Committed")

    def close_spider(self, spider):
        # Close database connection
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        # Insert item into the table
        try:
            insert_query = sql.SQL("""
                INSERT INTO cars_data (
                    detail_page_link, make, title, trim_name, car_images,
                    exterior_color, interior_color, transmission, engine,
                    certified_preowned, doors, stock_id, vin, list_price,
                    loan_estimate, mileage, dealer_info, carfaxOwnerNumber,
                    features, seller_comments, interest_rate, down_payment_percentage,
                    down_payment_value, dealer_name, dealer_address, dealer_phone,
                    dealer_website
                ) VALUES (
                    {values}
                )
            """).format(
                values=sql.SQL(", ").join(sql.Placeholder() * len(item))
            )
            self.cursor.execute(insert_query, tuple(item.values()))
            self.conn.commit()
        except Exception as e:
            spider.logger.error(f"Failed to insert item: {e}")
            raise DropItem(f"Failed to insert item: {item}")
        return item
