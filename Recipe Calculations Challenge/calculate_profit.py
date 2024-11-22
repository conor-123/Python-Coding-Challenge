import pandas

#Read all 3 csv Files
ingredients = pandas.read_csv("ingredients.csv")
items = pandas.read_csv("items.csv")
recipes = pandas.read_csv("recipes.csv")

#Rename Cost to Cost_Per_Unit for more clarity
items = items.rename(columns={"Cost": "Cost_Per_Unit"})

# Merge ingredients & items based on matching 'IngredientId' and 'ItemId'
#how= outer will return NaN (Therefore will include the 3 IngredientType recipe)
ingredients_items_merge = pandas.merge(ingredients, items, left_on='IngredientId', right_on='Id', how='outer')

#Create IngredientCost by mutliplying cost of item x quantity - This will give a IngredientCost for all Ingredient Type = item
ingredients_items_merge['IngredientCost'] = ingredients_items_merge['Cost_Per_Unit'] * ingredients_items_merge['Quantity']


# Search for matching IngredientIds in RecipeId column
# matching_rows = ingredients_items_merge[ingredients_items_merge['RecipeId'].isin(ingredients_items_merge['IngredientId'])]

#Change the Cost for a specific RecipeId - (Hard Coded)
# ingredients_items_merge.loc[ingredients_items_merge['IngredientId'] == 'JUOVPZNTGXGGQYDRGORWTNQZNQ', 'Cost'] = 100
# ingredients_items_merge.loc[ingredients_items_merge['IngredientId'] == 'TPQCUSKEILFLSPGNHASDFEKXXA', 'Cost'] = 200


#Search multiple IDs in dataframe, calculate overall cost, then update the overall cost to the dataframe
def update_costs(dataframe, id1_column, id2_column, cost_column):
    
    #Create subset of matching rows 
    matching_rows = dataframe[dataframe[id1_column].isin(dataframe[id2_column])]

    #Group matching rows by id1_col then calcuate sum of cost_column
    total_cost = matching_rows.groupby(id1_column, as_index=False)[cost_column].sum()
   
    #Split into a dict
    id_to_search = total_cost[id1_column].to_dict()
    cost_to_update = total_cost[cost_column].to_dict()
 

    #Use id_to_search as key , each key match return return the id and cost and combine to a dict
    col_mapping = {id_to_search[k]: cost_to_update[k] for k in id_to_search.keys()}

    #Update the Cost Column using the Id and Cost from col_mapping
    dataframe.loc[
        dataframe[id2_column].isin(col_mapping.keys()), cost_column
    ] = dataframe[id2_column].map(col_mapping)

    return dataframe


# Call the function to update the 'Cost' column
#Get the OverallCost for all RecipeIds IngredientCost will be updated to the cost of ALL Ingredients contained in a RecipeId
update_costs(ingredients_items_merge, 'RecipeId', 'IngredientId', 'IngredientCost')


#Merge Recipes.csv and ingredients_items_merge DataFrame together to subtract OverallCost to Profit
recipes_ingredients_merge = pandas.merge(ingredients_items_merge, recipes, left_on='RecipeId', right_on='Id', how='right')


#Where IngredientType = recipe multiply ingredientCost by Quantity
recipes_ingredients_merge.loc[
    recipes_ingredients_merge["IngredientType"].str.contains("recipe", case=False), "IngredientCost"
] = recipes_ingredients_merge["IngredientCost"] * recipes_ingredients_merge["Quantity"]


#Any IngredientType = recipe , combine the IngredientCost
total_recipe_cost = recipes_ingredients_merge.groupby('RecipeId', as_index=False)['IngredientCost'].sum()

#Merge the updated IngredientCost back into Dataframe - This will give the true 'Overall Cost for an IngredientType = recipe
recipes_ingredients_merge = pandas.merge(recipes_ingredients_merge, total_recipe_cost, on='RecipeId', suffixes=('', '_Total'))


#Sort alphabetically in descending order (z-a) - (Therefore IndgredientType = recipe will always be the first picked)
#IngredientType = recipe needs to be the first unique RecipeId picked because if the RecipeId also has IngredientType = item the Overall Cost will be incorrect
recipes_ingredients_merge = recipes_ingredients_merge.sort_values(by="IngredientType", ascending=False).drop_duplicates(subset='RecipeId', keep='first')


#If SalePrice is NaN - Replace with 0 (This will allow for valid calculations)
recipes_ingredients_merge['SalePrice'] = recipes_ingredients_merge['SalePrice'].fillna(0)


#Get Profit by Subtracting SalePrice from IngredientCost_Total
recipes_ingredients_merge['Profit'] = recipes_ingredients_merge['SalePrice'] - recipes_ingredients_merge['IngredientCost_Total']



#Rename Columns for nicer output
recipes_ingredients_merge = recipes_ingredients_merge.rename(
    columns={"Name_y": "Food", "IngredientCost_Total": "Total Cost", "SalePrice": "Sale Price"}
)

#Ensure all number columns print to two decimal places
recipes_ingredients_merge[["Total Cost", "Sale Price", "Profit"]] = recipes_ingredients_merge[["Total Cost", "Sale Price", "Profit"]].round(2)


#Print results and order it by highest Profit to lowest
profit = recipes_ingredients_merge[['Food', 'Total Cost', 'Sale Price', 'Profit']].sort_values(by='Profit', ascending=False)


print(profit)
profit.to_csv("profit.csv", index=False)




#Initial Notes / Rough work

#Example:

#Tendies --> Use unique 'Id'  on recipes.csv

#Tendies 'Id' is = DLEZVCCTYUQRRELGVYRFVAYFVP - Search this under 'RecipeId' heading on ingredients.csv (2 results)
#Grab the 'IngredientId' along with 'quantity' for any matching 'RecipeId'

#Then using the 'IngredientId' - Search this under 'Id' on items.csv
#Grab the 'Cost'

#Multiply the 'quantity' by the 'cost'

### Eg For Tendies - 

#Item 1 - Chicken Tender, Price: 0.78 x quantity of 8 = 6.24 euro
#Item 2 - Garlic Mayo, Price: 0.45 x quantity of 2 = 0.9 euro

#Combine overall cost 6.24 + 0.9 = 7.14

#Then subtract cost from SalePrice of Tendies 

#Overall Cost 7.14 - SalePrice 8.00 = 0.86 euro PROFIT


### What if IngredientType is a 'receipe' instead of 'item' in ingredients.csv

#Each IngredientType 'recipe' contains 'item' - 
#Therefore need to grab the cost of all 'item' that make up 'recipe' 
#Eg if it contains 3 'item' then get indivdual price per item and multiple by the quantity for 'recipe'
#Will need to have have a list of prices for all 'item' so we can grab from if 'recipe' is an ingredient

#Basically want to assign a COST to every IngredientType on ingredients.csv
#This will then be searched for using the Ids - eg Cost is already calculated for all IngrientTypes on ingredients.csv by comparing to items.csv
#Therefore search recipes from recipes.csv, getting the total cost of all IngredientType attached to the recipe and then subtracting it from the SalePrice