import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, first, rand
from sqlalchemy import create_engine

utilisateur_id = 1

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("MenuGeneration") \
    .getOrCreate()

# Chargement des données OpenFoodFacts depuis un fichier CSV
openfoodfacts_path = "C:/Users/cguil/Documents/I1/Intégration des données/data source/en.openfoodfacts.org.products.csv"
df_openfoodfacts = spark.read.csv(openfoodfacts_path, sep="\t", header=True, inferSchema=True)

# Chargement des données de régimes alimentaires depuis un fichier CSV
menus_path = "C:/Users/cguil/Documents/I1/Intégration des données/data source/les-menus.csv"
df_menus = spark.read.csv(menus_path, header=True, inferSchema=True)

# Chargement des données des utilisateurs depuis un fichier CSV
users_path = "C:/Users/cguil/Documents/I1/Intégration des données/data source/les-utilisateurs.csv"
df_users = spark.read.csv(users_path, header=True, inferSchema=True)

# Filtrer le DataFrame pour récupérer les informations sur l'utilisateur 1
region_utilisateur = df_users.filter(df_users["id_utilisateur"] == utilisateur_id).select("region").collect()[0][0]

# Filtrage des produits valides
df_valid_products = df_openfoodfacts.filter(
    col("product_name").isNotNull() &
    col("countries_en").isNotNull() &
    col("energy_100g").isNotNull() &
    col("fat_100g").isNotNull() &
    col("carbohydrates_100g").isNotNull() &
    col("proteins_100g").isNotNull() &
    col("categories").isNotNull() &
    (col("categories") != "NULL") &
    (col("cities_tags") != "NULL") &
    col("cities_tags").isNotNull() &
    col("categories_tags").isNotNull() &
    col("labels").isNotNull() &
    (~col("categories").like("%Condiments%")) &
    col("cities_tags").contains(region_utilisateur) & (
    ((col("categories").like("%Aliments%")) & col("categories_tags").like("%meats%")) |
    (col("categories").like("%Plats%")) |
    (col("categories").like("%Viandes%")) |
    (col("categories").like("%Préparés%")))
)

# Filtrage des menus valides
df_valid_menus = df_menus.filter(
    col("id_menu").isNotNull() &
    col("nom_menu").isNotNull() &
    col("seuil_glucides").isNotNull() &
    col("seuil_lipides").isNotNull() &
    col("seuil_proteines").isNotNull() &
    col("seuil_calories").isNotNull()
)

# Filtrage des utilisateurs valides
df_valid_users = df_users.filter(
    col("id_utilisateur").isNotNull() &
    col("id_menu").isNotNull() &
    col("nom_utilisateur").isNotNull() &
    col("age").isNotNull() &
    col("sexe").isNotNull() &
    col("poids").isNotNull()
)

#recupération des colonnes intéressantes
selected_columns_valid_products_result = df_valid_products.select("product_name","labels", "energy_100g","fat_100g","carbohydrates_100g","proteins_100g","categories")

# Jointure entre les DataFrames des utilisateurs et des menus sur la colonne "id_menu"
df_user_menu = df_users.join(df_menus, df_users["id_menu"] == df_menus["id_menu"], "inner")

# Sélection de certaines colonnes après la jointure
user_menu_join = df_user_menu.select(df_users["id_utilisateur"], df_menus["nom_menu"], df_menus["seuil_glucides"],  df_menus["seuil_lipides"], df_menus["seuil_proteines"], df_menus["seuil_calories"])

#jointure des 3 sources + filtre de seuil de composition en fonction du régime
joined_df = selected_columns_valid_products_result.join(
    user_menu_join,
    (selected_columns_valid_products_result["proteins_100g"] < user_menu_join["seuil_proteines"]) &
    (selected_columns_valid_products_result["carbohydrates_100g"] < user_menu_join["seuil_glucides"]) &
    (selected_columns_valid_products_result["fat_100g"] < user_menu_join["seuil_lipides"]) &
    (selected_columns_valid_products_result["energy_100g"] < user_menu_join["seuil_calories"]),
    "inner"
)

#filtre pour récupérer uniquement l'utilisateur demande
aliment_utilisateur = joined_df.filter(joined_df["id_utilisateur"] == utilisateur_id)

aliment_utilisateur.filter(col('nom_menu') == "Bio").filter(
    (col("labels").like("%Fait maison%")) |
    (col("labels").like("%bio%")) |
    (col("labels").like("%Point Vert%"))
).limit(100)

jours_semaine = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]

#Fonction de recherche aléatoire
def choisir_aleatoire(df):
    return df.orderBy(rand()).first()

# Création du DataFrame final pour le menu de la semaine
menu_semaine = []

for jour in jours_semaine:

    # Sélection aléatoire d'un plat pour le midi et le soir pour chaque jour
    produit_midi = choisir_aleatoire(aliment_utilisateur)
    produit_soir = choisir_aleatoire(aliment_utilisateur)

    menu_semaine.append((jour, produit_midi["product_name"], produit_soir["product_name"],utilisateur_id,produit_midi["proteins_100g"]+produit_soir["proteins_100g"],produit_midi["carbohydrates_100g"]+produit_soir["carbohydrates_100g"],produit_midi["fat_100g"]+produit_soir["fat_100g"],produit_midi["energy_100g"]+produit_soir["energy_100g"], ))

# Création du DataFrame Spark pour le menu de la semaine
df_menu_semaine = spark.createDataFrame(menu_semaine, ["Jour", "Midi", "Soir","Id-utilisateur", "Proteines-jour", "Glucides-jour","Lipides-jour", "Calories-jour"])

# Convertir en DataFrame Pandas
df_pandas = df_menu_semaine.toPandas()

# Créer la connexion
engine = create_engine('mysql+pymysql://root:@localhost/tp-integration')

# Écrire le DataFrame Pandas dans MySQL
df_pandas.to_sql(name='menu', con=engine, if_exists='replace', index=False)

# Arrêter la session Spark
spark.stop()
