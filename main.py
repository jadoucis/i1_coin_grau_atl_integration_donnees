from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import random

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("MenuGeneration") \
    .getOrCreate()

# Chargement des données OpenFoodFacts depuis un fichier CSV
openfoodfacts_path = "C:/Users/cguil/Documents/I1/Intégration des données/data source/en.openfoodfacts.org.products.csv"
df_openfoodfacts = spark.read.csv(openfoodfacts_path,sep="\t", header=True, inferSchema=True)

# Chargement des données de régimes alimentaires depuis un fichier CSV
regimes_path = "C:/Users/cguil/Documents/I1/Intégration des données/data source/les-menus.csv"
df_regimes = spark.read.csv(regimes_path, header=True, inferSchema=True)

# Chargement des données des utilisateurs depuis un fichier CSV
users_path = "C:/Users/cguil/Documents/I1/Intégration des données/data source/les-utilisateurs.csv"
df_users = spark.read.csv(users_path,  header=True, inferSchema=True)
#
# Filtrage des produits valides
df_valid_products = df_openfoodfacts.filter(
    col("product_name").isNotNull() &
    col("region").isNotNull() &
    col("energy_100g").isNotNull() &
    col("fat_100g").isNotNull() &
    col("carbohydrates_100g").isNotNull() &
    col("proteins_100g").isNotNull()
)
df_valid_products.limit(100).show(truncate=False)
#
# # Génération aléatoire du menu pour chaque utilisateur
# def generate_menu(user_id, regime):
#     # Filtrer les produits selon le régime
#     regime_df = df_regimes.filter(col("nom") == regime).first()
#     menu = df_valid_products.filter(
#         col("carbohydrates_100g") < regime_df["seuil_glucides"] &
#         col("fat_100g") < regime_df["seuil_lipides"] &
#         col("proteins_100g") > regime_df["seuil_proteines"]
#     ).sample(False, 0.2).limit(7)  # Échantillonnage aléatoire de 7 produits pour une semaine
#     menu = menu.select("product_name", "region", "energy_100g", "fat_100g", "carbohydrates_100g", "proteins_100g")
#     menu = menu.withColumn("user_id", user_id)
#     return menu
#
# menus = []
# for row in df_users.collect():
#     user_id = row["id_menu"]
#     regime = row["nom"]
#     menus.append(generate_menu(user_id, regime))
#
# # Union des menus générés pour tous les utilisateurs
# df_menu = menus[0]
# for menu in menus[1:]:
#     df_menu = df_menu.union(menu)
#
# # Dépôt des résultats dans le Datawarehouse (hypothétique)
# # df_menu.write.format("jdbc").options(
# #     url="jdbc:mysql://your-host:your-port/your-database",
# #     driver="com.mysql.jdbc.Driver",
# #     dbtable="menu",
# #     user="your-username",
# #     password="your-password"
# # ).mode("append").save()
#
# # Affichage des menus générés
# df_menu.show()
#
# # Arrêt de la session Spark
# spark.stop()