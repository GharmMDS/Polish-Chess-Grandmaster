import sys
import os
import subprocess

def main():
    username = input("Enter the Chess.com username to process: ").strip().lower()
    data_folder = username

    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = scripts_dir

    fetch_data_path = os.path.join(scripts_dir, "scripts", "fetch_data.py")  # Added "scripts"
    dates_path = os.path.join(scripts_dir, "data", "dates.py")            # Added "scripts"
    extract_dates_db_path = os.path.join(root_dir, "scripts", "connection_to_database.py")
    analyze_data_path = os.path.join(scripts_dir, "scripts", "analyze_data.py")  # Added "scripts"
    visualize_path = os.path.join(scripts_dir, "scripts", "visualize.py")   # Added "scripts"
    connection_to_db_path = os.path.join(scripts_dir, "scripts", "connection_to_database.py")  # Added connection_to_database.py

    # Debugging paths (you can comment this out when not needed)
    # print("--- Path Debugging ---")
    # print(f"Scripts Directory: {scripts_dir}")
    # print(f"Root Directory: {root_dir}")
    # print(f"Fetch Data Path: {fetch_data_path}")
    # print(f"Dates Path: {dates_path}")
    # print(f"Extract Dates DB Path: {extract_dates_db_path}")
    # print(f"Analyze Data Path: {analyze_data_path}")
    # print(f"Visualize Path: {visualize_path}")
    # print(f"Connection to DB Path: {connection_to_db_path}")
    # print("----------------------")

    # Step 1: Fetch game data and update the database
    print(f"🔍 Fetching game data for {username} and updating database...")
    subprocess.run([sys.executable, fetch_data_path, username])
    print("✅ Game data fetching and initial database update complete.")

    # Step 2: Insert data into the database (connection_to_database.py)
    print(f"\n🔄 Inserting data into the database for {username}...")
    subprocess.run([sys.executable, connection_to_db_path, username])
    print("✅ Database updated with fetched game data.")

    # Step 3: Extract dates from PGNs for further analysis
    print(f"\n🗓️ Extracting dates from PGNs for {username}...")
    subprocess.run([sys.executable, dates_path, username])  # 👈 add username
    print("✅ Date extraction complete. Dates saved to data/extracted_dates.csv")

    # Step 4: Update the database with the extracted dates
    print("\n🔄 Updating database with extracted dates...")
    subprocess.run([sys.executable, extract_dates_db_path, username])  # 👈 add username
    print("✅ Database updated with extracted dates.")


    # Step 5: Analyze data
    print(f"\n📊 Analyzing data for {username}...")
    subprocess.run([sys.executable, analyze_data_path, username, data_folder])
    print("✅ Analysis complete.")

    # Step 6: Visualize data
    print(f"\n📈 Visualizing data for {username}...")
    subprocess.run([sys.executable, visualize_path, username, data_folder])
    print("✅ Visualization complete.")

    print("\n✨ Project workflow complete.")

if __name__ == "__main__":
    main()
