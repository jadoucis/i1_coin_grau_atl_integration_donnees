import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, first
from sqlalchemy import create_engine

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
#
# Filtrage des produits valides
df_valid_products = df_openfoodfacts.filter(
    col("product_name").isNotNull() &
    col("countries_en").isNotNull() &
    col("energy_100g").isNotNull() &
    col("fat_100g").isNotNull() &
    col("carbohydrates_100g").isNotNull() &
    col("proteins_100g").isNotNull() &
    col("categories").isNotNull() &
    col("cities_tags").isNotNull()
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
# création d'une nouvelle colonne "categorie_label" qui récupère uniquement l'élément interessant
df_result = df_valid_products.withColumn("categorie_label", split(df_valid_products["categories"], ",").getItem(0))

#Sélection des colonne visible dans le dataFrame
selected_columns_df = df_result.select("product_name", "countries_en", "energy_100g","fat_100g","carbohydrates_100g","proteins_100g","categorie_label").limit(10)
#selected_columns_df.limit(100).show(truncate=False)

# connection = pymysql.connect(
#     host='localhost',
#     user='root',
#     password='',
#     database='tp-integration',
#     charset='utf8mb4',
#     cursorclass=pymysql.cursors.DictCursor
# )

# Convertir le DataFrame Spark en DataFrame Pandas
df_pandas = selected_columns_df.toPandas()

# Créer le moteur SQLAlchemy avec le pilote MySQL de PyMySQL
engine = create_engine('mysql+pymysql://root:@localhost/tp-integration')


# Étape 3 : Écrire le DataFrame Pandas dans MySQL
df_pandas.to_sql(name='menu', con=engine, if_exists='replace', index=False)

#






# df_result["categorie_label"].limit(100).show(truncate=False)


#df_valid_products.limit(100).show(truncate=False)
#
# df_valid_users.limit(100).show(truncate=False)
#
# df_valid_menus.limit(100).show(truncate=False)
# Arrêter la session Spark
spark.stop()



