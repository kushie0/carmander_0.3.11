from scrape import KBBScraper

url2 = ' https://www.kbb.com/rest/lsc/listing?listingType=USED&startYear=2020&endYear=2020&year=%5Bobject%20Object%5D&city=Binghamton&state=NY&zip=13901&location=%5Bobject%20Object%5D&firstRecord=100&newSearch=false&makeCode=HONDA%2CHYUND%2CTOYOTA&marketExtension=off&numRecords=100&searchRadius=500&sortBy=derivedpriceASC&dma=%5Bobject%20Object%5D&channel=KBB&relevanceConfig=default&vhrProviders=EXPERIAN&vhrProvider=EXPERIAN&stats=year%2Cderivedprice'


def test():
    bing_scrape = KBBScraper()
    bing_scrape.run_scrape(max_cars=50000)


if __name__ == '__main__':
    test()
