# import pandas as pd

# df = pd.read_csv("init_setups/scraped_data.csv")

# df['Name'] = df['Name'].str.replace('...', '', regex=False)

# df.to_csv('data_generation/merchants.csv', index=False)

# # Print the cleaned data to check
# print(df['Name'])


import pandas as pd

# Load the CSV into a DataFrame
df = pd.read_csv('init_setups/restaurants.csv')

# Remove the 'Address:' prefix from the Address column
df['Address'] = df['Address'].str.replace('Address: ', '', regex=False)

# Save the cleaned data back to a CSV file
df.to_csv('data_generation/restaurants_cleaned.csv', index=False)