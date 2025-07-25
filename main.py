#from utils.ip_process import process_ip_locations
from utils.product_processing import get_product_data
from utils.crawl_product_names import crawl_products

if __name__ == "__main__":
    # print("Extract ip loacation data ...")
    # process_ip_locations() 
    
    print("Extract product id and current url data ...")
    get_product_data()
    #crawl_products()
    print("Done!")
    

