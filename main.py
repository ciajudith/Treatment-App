from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,month,year,to_date

spark = SparkSession.builder.appName("Analyse de Vente").getOrCreate()

df_orders = spark.read.option("delimiter", ",").csv("sales-2016.csv", header=True)
df_orders = df_orders.withColumn("SalesQuantity", col("SalesQuantity").cast("int")) \
        .withColumn('Date', to_date(col('Date'), 'dd/MM/yyyy')) \
        .withColumn("UnitCost", col("UnitCost").cast("float")) \
        .withColumn("UnitPrice", col("UnitPrice").cast("float")) \
        .withColumn("TotalCost", col("TotalCost").cast("float")) \
        .withColumn("TotalPrice", col("TotalPrice").cast("float")) \
        .withColumn("TotalProfit", col("TotalProfit").cast("float"))
    
df_orders.show()

df_products = spark.read.option("delimiter", "\t").csv("produits.txt", header=True)
df_products.show()


def visualiser_ventes_par_produit():    
    print("Visualisation du nombre de ventes par produit.")
    df_combined = df_orders.join(df_products, df_orders.Product == df_products.Produit, "inner")
    resultats = df_combined.groupBy("Product").agg(sum("SalesQuantity").alias("NombreVentes"))
    resultats.show()

def visualiser_chiffre_affaire():
    print("Visualisation du chiffre d'affaires.")
    resultats = df_orders.groupBy().agg(sum("TotalPrice").alias("ChiffreAffaires"))
    resultats.show()

def visualiser_evolution_ventes():
    print("Visualisation de l'évolution des ventes au fil des mois ou des années.")
    df_order = df_orders.withColumn("Mois", month("Date"))
    df_order.show()
    resultats = df_order.groupBy("Mois").agg(sum("SalesQuantity").alias("NombreVentes"))

    resultats.show()


def visualiser_ventes_par_categorie():
    print("Visualisation des ventes par catégories de produit.")
    df_combined = df_orders.join(df_products, df_orders.Product == df_products.Produit, "inner")
    resultats = df_combined.groupBy("Categories").agg(sum("SalesQuantity").alias("NombreVentesParCategorie"))

    resultats.show()

def main():
    
    while True:
        print("\nOptions :")
        print("1. Visualiser le nombre de ventes par produit")
        print("2. Visualiser le chiffre d'affaires")
        print("3. Visualiser l'évolution des ventes")
        print("4. Visualiser les ventes par catégories de produit")
        print("0. Quitter")

        choix = input("Choisissez une option (0-4) : ")

        actions = {
            '1': visualiser_ventes_par_produit,
            '2': visualiser_chiffre_affaire,
            '3': visualiser_evolution_ventes,
            '4': visualiser_ventes_par_categorie,
            '0': exit
        }
    
        action = actions.get(choix, lambda: print("Option invalide. Veuillez choisir à nouveau."))
        action()
        

if __name__ == "__main__":
    main()
    spark.stop()
