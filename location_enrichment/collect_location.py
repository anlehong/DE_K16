import IP2Location

def collect_ip_location(ip_address):
    ipdb = IP2Location.IP2Location("data/IP-COUNTRY-REGION-CITY.BIN")
    info = ipdb.get_all(ip_address)
    try:
        info = ipdb.get_all(ip_address)
        return {
            "ip": ip_address,
            "country_short": info.country_short,
            "country": info.country_long,
            "region": info.region,
            "city": info.city,
        }
    except Exception as e:
        return {
            "ip": ip_address,
            "error": str(e)
        }