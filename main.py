from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

def product_category_pairs(products: DataFrame, categories: DataFrame, product_categories: DataFrame) -> DataFrame:
    # Соединяем product_categories с категориями для получения category_name
    pc_with_cat = product_categories.join(categories, "category_id", "left") \
                                    .select("product_id", "category_name")
    
    # Левый джойн продуктов с product_categories + категории
    prod_with_cat = products.join(pc_with_cat, "product_id", "left") \
                            .select("product_name", "category_name")
    
    # Продукты без категорий будут иметь category_name = null
    return prod_with_cat