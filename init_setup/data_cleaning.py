import pandas as pd

# Load the CSV into a DataFrame
df = pd.read_csv("init_setups/restaurants.csv")

# Remove the 'Address:' prefix from the Address column
df["Address"] = df["Address"].str.replace("Address: ", "", regex=False)

# Save the cleaned data back to a CSV file
df.to_csv("data_generation/restaurants_cleaned.csv", index=False)
