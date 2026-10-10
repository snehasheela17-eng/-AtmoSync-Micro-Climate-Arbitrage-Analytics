import os
import snowflake.connector
from dotenv import load_dotenv


# =========================================================
# LOAD ENVIRONMENT VARIABLES
# =========================================================

load_dotenv()

password = os.getenv("SNOWFLAKE_PASSWORD")

if not password:
    print("ERROR: SNOWFLAKE_PASSWORD not found in .env file.")
    exit()


# =========================================================
# SNOWFLAKE CONNECTION
# =========================================================

try:

    conn = snowflake.connector.connect(
        account="KXSJJKP-YE27378",
        user="snehasheela",
        password=password,
        role="ACCOUNTADMIN",
        database="ATMOSYNC_DB",
        schema="ANALYTICS"
    )

    print("\n✓ Connected to Snowflake successfully.")

except Exception as e:

    print("\nERROR: Could not connect to Snowflake.")
    print(e)
    exit()


cursor = conn.cursor()


# =========================================================
# MAIN MENU
# =========================================================

while True:

    print("\n================================")
    print("        ATMOSYNC PROJECT")
    print("================================")

    print("\n1. Container Risk Analysis")
    print("2. Market Analysis")
    print("3. Arbitrage Analysis")
    print("4. Spoilage Analysis")
    print("5. Exit")

    choice = input("\nEnter your choice: ")


    # =====================================================
    # 1. CONTAINER RISK ANALYSIS
    # =====================================================

    if choice == "1":

        print("\n===== CONTAINER RISK ANALYSIS =====")

        print("1. C001")
        print("2. C002")
        print("3. C003")
        print("4. C004")
        print("5. C005")
        print("6. All Containers")

        container_choice = input("\nEnter container choice: ")

        container_map = {
            "1": "C001",
            "2": "C002",
            "3": "C003",
            "4": "C004",
            "5": "C005"
        }


        if container_choice in container_map:

            container_id = container_map[container_choice]

            cursor.execute("""
                SELECT
                    CONTAINER_ID,
                    COUNT(*) AS TOTAL_READINGS,

                    SUM(
                        CASE
                            WHEN ENVIRONMENT_RISK = 'HIGH_RISK'
                            THEN 1 ELSE 0
                        END
                    ) AS HIGH_RISK,

                    SUM(
                        CASE
                            WHEN ENVIRONMENT_RISK = 'MEDIUM_RISK'
                            THEN 1 ELSE 0
                        END
                    ) AS MEDIUM_RISK,

                    SUM(
                        CASE
                            WHEN ENVIRONMENT_RISK = 'LOW_RISK'
                            THEN 1 ELSE 0
                        END
                    ) AS LOW_RISK

                FROM ATMOSYNC_DB.ANALYTICS.CONTAINER_HEALTH

                WHERE CONTAINER_ID = %s

                GROUP BY CONTAINER_ID

            """, (container_id,))

            row = cursor.fetchone()


            if row:

                print(f"\n===== {container_id} RISK ANALYSIS =====")

                print("Total Readings :", row[1])
                print("High Risk      :", row[2])
                print("Medium Risk    :", row[3])
                print("Low Risk       :", row[4])


                if row[2] > 0:
                    risk = "HIGH_RISK"

                elif row[3] > 0:
                    risk = "MEDIUM_RISK"

                else:
                    risk = "LOW_RISK"


                print("Overall Risk   :", risk)

                print("--------------------------------")


            else:

                print("\nNo data found for", container_id)


        elif container_choice == "6":

            cursor.execute("""
                SELECT
                    CONTAINER_ID,
                    COUNT(*) AS TOTAL_READINGS,

                    SUM(
                        CASE
                            WHEN ENVIRONMENT_RISK = 'HIGH_RISK'
                            THEN 1 ELSE 0
                        END
                    ) AS HIGH_RISK,

                    SUM(
                        CASE
                            WHEN ENVIRONMENT_RISK = 'MEDIUM_RISK'
                            THEN 1 ELSE 0
                        END
                    ) AS MEDIUM_RISK,

                    SUM(
                        CASE
                            WHEN ENVIRONMENT_RISK = 'LOW_RISK'
                            THEN 1 ELSE 0
                        END
                    ) AS LOW_RISK

                FROM ATMOSYNC_DB.ANALYTICS.CONTAINER_HEALTH

                GROUP BY CONTAINER_ID

                ORDER BY CONTAINER_ID

            """)

            results = cursor.fetchall()

            print("\n===== ALL CONTAINERS =====")


            for row in results:

                if row[2] > 0:
                    risk = "HIGH_RISK"

                elif row[3] > 0:
                    risk = "MEDIUM_RISK"

                else:
                    risk = "LOW_RISK"


                print("\nContainer ID  :", row[0])
                print("Total Readings:", row[1])
                print("High Risk     :", row[2])
                print("Medium Risk   :", row[3])
                print("Low Risk      :", row[4])
                print("Overall Risk  :", risk)

                print("--------------------------------")


        else:

            print("\nInvalid container choice.")


    # =====================================================
    # 2. MARKET ANALYSIS
    # =====================================================

    elif choice == "2":

        print("\n===== MARKET ANALYSIS =====")


        # Get cities

        cursor.execute("""
            SELECT DISTINCT CITY
            FROM ATMOSYNC_DB.ANALYTICS.DAILY_MARKET_SUMMARY
            ORDER BY CITY
        """)

        cities = cursor.fetchall()


        print("\nSelect City:")

        for i, row in enumerate(cities, start=1):
            print(f"{i}. {row[0]}")


        city_choice = input("\nEnter city choice: ")


        if not city_choice.isdigit():

            print("\nInvalid city choice.")
            continue


        city_number = int(city_choice)


        if city_number < 1 or city_number > len(cities):

            print("\nInvalid city choice.")
            continue


        selected_city = cities[city_number - 1][0]


        # Get products

        cursor.execute("""
            SELECT DISTINCT PRODUCT
            FROM ATMOSYNC_DB.ANALYTICS.DAILY_MARKET_SUMMARY

            WHERE CITY = %s

            ORDER BY PRODUCT

        """, (selected_city,))


        products = cursor.fetchall()


        print(f"\n===== {selected_city} =====")

        print("\nSelect Product:")


        for i, row in enumerate(products, start=1):
            print(f"{i}. {row[0]}")


        product_choice = input("\nEnter product choice: ")


        if not product_choice.isdigit():

            print("\nInvalid product choice.")
            continue


        product_number = int(product_choice)


        if product_number < 1 or product_number > len(products):

            print("\nInvalid product choice.")
            continue


        selected_product = products[product_number - 1][0]


        # Get final market result

        cursor.execute("""
            SELECT
                PRODUCT,
                CATEGORY,
                ROUND(AVG(AVG_SUPPLY_LEVEL), 2),
                ROUND(AVG(AVG_DEMAND_LEVEL), 2),
                ROUND(AVG(AVG_PRICE_INR), 2),
                ROUND(AVG(AVG_PRICE_CHANGE_PCT), 2)

            FROM ATMOSYNC_DB.ANALYTICS.DAILY_MARKET_SUMMARY

            WHERE CITY = %s
              AND PRODUCT = %s

            GROUP BY PRODUCT, CATEGORY

        """, (selected_city, selected_product))


        result = cursor.fetchone()


        if result:

            print(
                f"\n===== {selected_city} - "
                f"{selected_product} ====="
            )

            print("Product              :", result[0])
            print("Category             :", result[1])
            print("Average Supply       :", result[2])
            print("Average Demand       :", result[3])
            print("Average Price        : ₹", result[4])
            print("Average Price Change :", result[5], "%")

            print("--------------------------------")


        else:

            print("\nNo market data found.")


    # =====================================================
    # 3. ARBITRAGE ANALYSIS
    # =====================================================

    elif choice == "3":

        print("\n===== ARBITRAGE ANALYSIS =====")


        # Get source cities

        cursor.execute("""
            SELECT DISTINCT SOURCE_CITY
            FROM ATMOSYNC_DB.ANALYTICS.ARBITRAGE_ANALYSIS
            ORDER BY SOURCE_CITY
        """)

        source_cities = cursor.fetchall()


        print("\nSelect Source City:")


        for i, row in enumerate(source_cities, start=1):
            print(f"{i}. {row[0]}")


        source_choice = input("\nEnter source city choice: ")


        if not source_choice.isdigit():

            print("\nInvalid source city choice.")
            continue


        source_number = int(source_choice)


        if source_number < 1 or source_number > len(source_cities):

            print("\nInvalid source city choice.")
            continue


        selected_source = source_cities[source_number - 1][0]


        # Get destination cities

        cursor.execute("""
            SELECT DISTINCT DESTINATION_CITY
            FROM ATMOSYNC_DB.ANALYTICS.ARBITRAGE_ANALYSIS

            WHERE SOURCE_CITY = %s

            ORDER BY DESTINATION_CITY

        """, (selected_source,))


        destinations = cursor.fetchall()


        print(f"\n===== {selected_source} =====")

        print("\nSelect Destination City:")


        for i, row in enumerate(destinations, start=1):
            print(f"{i}. {row[0]}")


        destination_choice = input(
            "\nEnter destination choice: "
        )


        if not destination_choice.isdigit():

            print("\nInvalid destination choice.")
            continue


        destination_number = int(destination_choice)


        if (
            destination_number < 1
            or destination_number > len(destinations)
        ):

            print("\nInvalid destination choice.")
            continue


        selected_destination = destinations[
            destination_number - 1
        ][0]


        # Get products

        cursor.execute("""
            SELECT DISTINCT PRODUCT
            FROM ATMOSYNC_DB.ANALYTICS.ARBITRAGE_ANALYSIS

            WHERE SOURCE_CITY = %s
              AND DESTINATION_CITY = %s

            ORDER BY PRODUCT

        """, (
            selected_source,
            selected_destination
        ))


        products = cursor.fetchall()


        print(
            f"\n===== {selected_source} → "
            f"{selected_destination} ====="
        )

        print("\nSelect Product:")


        for i, row in enumerate(products, start=1):
            print(f"{i}. {row[0]}")


        product_choice = input(
            "\nEnter product choice: "
        )


        if not product_choice.isdigit():

            print("\nInvalid product choice.")
            continue


        product_number = int(product_choice)


        if product_number < 1 or product_number > len(products):

            print("\nInvalid product choice.")
            continue


        selected_product = products[
            product_number - 1
        ][0]


        # Get best/latest arbitrage result

        cursor.execute("""
            SELECT
                DATE,
                SOURCE_CITY,
                DESTINATION_CITY,
                PRODUCT,
                CATEGORY,
                ROUND(SOURCE_PRICE, 2),
                DISTANCE_KM,
                ROUND(TRANSPORT_COST, 2),
                ROUND(NET_PRICE_AFTER_TRANSPORT, 2),
                ROUND(AVG_DEMAND_LEVEL, 2),
                ROUND(AVG_SUPPLY_LEVEL, 2),
                ARBITRAGE_OPPORTUNITY

            FROM ATMOSYNC_DB.ANALYTICS.ARBITRAGE_ANALYSIS

            WHERE SOURCE_CITY = %s
              AND DESTINATION_CITY = %s
              AND PRODUCT = %s

            ORDER BY DATE DESC

            LIMIT 1

        """, (
            selected_source,
            selected_destination,
            selected_product
        ))


        result = cursor.fetchone()


        if result:

            print(
                "\n===== ARBITRAGE RESULT ====="
            )

            print("Date              :", result[0])
            print("Source City       :", result[1])
            print("Destination City  :", result[2])
            print("Product           :", result[3])
            print("Category          :", result[4])
            print("Source Price      : ₹", result[5])
            print("Distance          :", result[6], "km")
            print("Transport Cost    : ₹", result[7])
            print("Net Value         : ₹", result[8])
            print("Demand            :", result[9])
            print("Supply            :", result[10])
            print("Opportunity       :", result[11])

            print("--------------------------------")


        else:

            print("\nNo arbitrage data found.")


    # =====================================================
    # 4. SPOILAGE ANALYSIS
    # =====================================================

    elif choice == "4":

        print("\n===== SPOILAGE ANALYSIS =====")


        # Get source cities

        cursor.execute("""
            SELECT DISTINCT SOURCE_CITY
            FROM ATMOSYNC_DB.ANALYTICS.SPOILAGE_ARBITRAGE
            ORDER BY SOURCE_CITY
        """)

        source_cities = cursor.fetchall()


        print("\nSelect Source City:")


        for i, row in enumerate(source_cities, start=1):
            print(f"{i}. {row[0]}")


        source_choice = input("\nEnter source city choice: ")


        if not source_choice.isdigit():

            print("\nInvalid source city choice.")
            continue


        source_number = int(source_choice)


        if source_number < 1 or source_number > len(source_cities):

            print("\nInvalid source city choice.")
            continue


        selected_source = source_cities[source_number - 1][0]


        # Get destination cities

        cursor.execute("""
            SELECT DISTINCT DESTINATION_CITY
            FROM ATMOSYNC_DB.ANALYTICS.SPOILAGE_ARBITRAGE

            WHERE SOURCE_CITY = %s

            ORDER BY DESTINATION_CITY

        """, (selected_source,))


        destinations = cursor.fetchall()


        print(f"\n===== {selected_source} =====")

        print("\nSelect Destination City:")


        for i, row in enumerate(destinations, start=1):
            print(f"{i}. {row[0]}")


        destination_choice = input(
            "\nEnter destination choice: "
        )


        if not destination_choice.isdigit():

            print("\nInvalid destination choice.")
            continue


        destination_number = int(destination_choice)


        if (
            destination_number < 1
            or destination_number > len(destinations)
        ):

            print("\nInvalid destination choice.")
            continue


        selected_destination = destinations[
            destination_number - 1
        ][0]


        # Get products

        cursor.execute("""
            SELECT DISTINCT PRODUCT
            FROM ATMOSYNC_DB.ANALYTICS.SPOILAGE_ARBITRAGE

            WHERE SOURCE_CITY = %s
              AND DESTINATION_CITY = %s

            ORDER BY PRODUCT

        """, (
            selected_source,
            selected_destination
        ))


        products = cursor.fetchall()


        print(
            f"\n===== {selected_source} → "
            f"{selected_destination} ====="
        )

        print("\nSelect Product:")


        for i, row in enumerate(products, start=1):
            print(f"{i}. {row[0]}")


        product_choice = input(
            "\nEnter product choice: "
        )


        if not product_choice.isdigit():

            print("\nInvalid product choice.")
            continue


        product_number = int(product_choice)


        if product_number < 1 or product_number > len(products):

            print("\nInvalid product choice.")
            continue


        selected_product = products[
            product_number - 1
        ][0]


        # Get final spoilage result

        cursor.execute("""
            SELECT
                DATE,
                SOURCE_CITY,
                DESTINATION_CITY,
                PRODUCT,
                CATEGORY,
                SOURCE_PRICE,
                DISTANCE_KM,
                STORAGE_RISK,
                ESTIMATED_SPOILAGE_HOURS,
                ROUND(ESTIMATED_TRAVEL_HOURS, 2),
                ROUND(NET_VALUE_AFTER_TRANSPORT, 2),
                SPOILAGE_FEASIBILITY,
                SPOILAGE_ARBITRAGE_OPPORTUNITY

            FROM ATMOSYNC_DB.ANALYTICS.SPOILAGE_ARBITRAGE

            WHERE SOURCE_CITY = %s
              AND DESTINATION_CITY = %s
              AND PRODUCT = %s

            ORDER BY DATE DESC

            LIMIT 1

        """, (
            selected_source,
            selected_destination,
            selected_product
        ))


        result = cursor.fetchone()


        if result:

            print(
                "\n===== SPOILAGE ARBITRAGE RESULT ====="
            )

            print("Date                 :", result[0])
            print("Source City          :", result[1])
            print("Destination City     :", result[2])
            print("Product              :", result[3])
            print("Category             :", result[4])
            print("Source Price         : ₹", result[5])
            print("Distance             :", result[6], "km")
            print("Storage Risk         :", result[7])
            print(
                "Spoilage Window      :",
                result[8],
                "hours"
            )
            print(
                "Estimated Travel     :",
                result[9],
                "hours"
            )
            print(
                "Net Value            : ₹",
                result[10]
            )
            print(
                "Spoilage Feasibility :",
                result[11]
            )
            print(
                "Opportunity           :",
                result[12]
            )

            print("--------------------------------")


        else:

            print("\nNo spoilage data found.")


    # =====================================================
    # 5. EXIT
    # =====================================================

    elif choice == "5":

        print("\nExiting AtmoSync...")
        break


    # =====================================================
    # INVALID MAIN MENU OPTION
    # =====================================================

    else:

        print("\nInvalid choice. Please select 1 to 5.")


# =========================================================
# CLOSE CONNECTION
# =========================================================

cursor.close()
conn.close()

print("\nSnowflake connection closed.")