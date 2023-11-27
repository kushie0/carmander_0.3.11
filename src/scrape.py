import sqlite3
import hrequests
import pandas as pd


def run_test():
    test4()


def test1():
    bing_scrape = KBBScraper(
        zip_code='13901',
        make_code='HONDA%2CHYUND%2CTOYOTA',
        min_year='2016',
        max_year='2023',
        max_mileage='75000',
        max_price='20000'
    )
    bing_scrape.run_scrape(max_cars=3000)


def test2():
    bing_scrape = KBBScraper(
        zip_code='13901',
        make_code='HONDA%2CHYUND%2CTOYOTA',
        min_year='2020',
        max_year='2020'
    )
    bing_scrape.run_scrape()


def test3():
    bing_scrape = KBBScraper(
        zip_code='13901',
        make_code='HONDA%2CHYUND%2CTOYOTA',
        min_year='2013',
        min_price='21000'
    )
    bing_scrape.run_scrape()


def test4():
    bing_scrape = KBBScraper(
        zip_code='12180',
        make_code='AUDI',
        min_year='2015'
    )
    bing_scrape.run_scrape()


class KBBScraper:
    ''' Performs an async scrape from KBB and stores results in a database.
    Will hopefully get data from a 500 mile radius.
    "In the midst of chaos, there is also opportunity." - Sun Tzu
    '''

    def __init__(
        self,
        zip_code=None,
        make_code=None,
        model_code=None,  # need to implement list
        min_year=None,
        max_year=None,
        min_mileage=None,
        max_mileage=None,
        min_price=None,
        max_price=None,
    ):
        self._url = 'https://www.kbb.com/rest/lsc/listing'
        self._base_parameters = self._get_base_parameters(
            zip_code=zip_code,
            make_code=make_code,
            model_code=model_code,
            min_year=min_year,
            max_year=max_year,
            min_mileage=min_mileage,
            max_mileage=max_mileage,
            min_price=min_price,
            max_price=max_price,
        )
        self._all_listings = []
        self._db_connection = sqlite3.connect('../data/carmander.db')

    def _fetch_kbb_listings(self, session, base_url, parameters: dict) -> list:
        params = [f'{key}={val}' for key, val in parameters.items()]
        params = '?' + '&'.join(params)
        url = base_url + params
        # print(url, len(self._all_listings), end='\n\n')
        print(
            f"Requesting: {base_url=} {parameters.get('minPrice')=} {parameters.get('firstRecord')=}")
        resp = session.get(url=url)
        my_page = resp.render(mock_human=True)

        # print(
        #     f'Elapsed!!\n{resp.elapsed.microseconds/10e6=} seconds', end='\n\n')

        if not resp.ok:
            raise ConnectionError(f'Failed fetch. {resp.status_code=}')
        json_data = resp.json()
        if not isinstance(json_data, dict) or 'listings' not in json_data:
            print(f'Listings missing. ({json_data.keys()=}))')
            return []
        listings = json_data['listings']
        if not isinstance(listings, list):
            print(f'Listings invalid. ({type(listings)=})')
            return []
        return listings

    def _flatten_kbb_car(self, car: dict) -> dict:
        '''This method is used to flatten the JSON response from KBB.
        The dict returned from this method is just str keys, str values.
        '''
        # iterate items, only acting on lists or dicts, ignoring str/int/float
        new_car = {}
        for key, value in car.items():
            if isinstance(value, list):
                # lists - only if they don't store more lists or dicts
                any_are_list = any([isinstance(x, list) for x in value])
                any_are_dict = any([isinstance(x, dict) for x in value])
                if any_are_list or any_are_dict:
                    pass
                else:
                    values = car[key]
                    new_car[key] = ';'.join(values)
            # dicts - only if they match these four specific keys
            elif isinstance(value, dict):
                if key == 'description':
                    new_car[key] = car[key]['label']
                elif key == 'phone':
                    new_car[key] = car[key]['value']
                elif key == 'pricingDetail':
                    new_car[key] = car[key]['salePrice']
                # 'specifications' contains more dicts e.g. {label: value}
                elif key == 'specifications':
                    specifications = car[key]
                    for spec_key, spec_value in specifications.items():
                        spec_key = 'spec_' + spec_key
                        new_car[spec_key] = spec_value['value']
            # we don't care about any other dicts
            else:
                new_car[key] = value
        return new_car

    def _flatten_kbb_car_new(self, car: dict, parent_key='', sep='_') -> dict:
        items = []
        for k, v in car.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_kbb_car_new(
                    v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if v and isinstance(v[0], dict):
                    for i, sub_dict in enumerate(v):
                        items.extend(self._flatten_kbb_car_new(
                            sub_dict, f"{new_key}_{i}", sep=sep).items())
                else:
                    # Join all list items into a string, assuming they are not dictionaries
                    items.append((new_key, ', '.join(str(x) for x in v)))
            else:
                items.append((new_key, str(v)))
        return dict(items)

    def _get_base_parameters(
        self,
        zip_code,
        make_code,
        model_code,
        min_year,
        max_year,
        min_mileage,
        max_mileage,
        min_price,
        max_price
    ):
        # if variables are list, join them with '%2C'
        if make_code and isinstance(make_code, list):
            make_code = '%2C'.join([str(code) for code in make_code])
        if model_code and isinstance(model_code, list):
            model_code = '%2C'.join([str(code) for code in model_code])
        # if variables are not None, convert them to string
        zip_code = str(zip_code) if zip_code else zip_code
        min_year = str(min_year) if min_year else min_year
        max_year = str(max_year) if max_year else max_year
        min_mileage = str(min_mileage) if min_mileage else min_mileage
        max_mileage = str(max_mileage) if max_mileage else max_mileage
        min_price = str(min_price) if min_price else min_price
        max_price = str(max_price) if max_price else max_price

        if isinstance(make_code, list):
            make_code = '%2C'.join(make_code)
        base_parameters = {
            'listingType': 'USED',
            'minPrice': min_price,
            'maxPrice': max_price,
            'price': '%5Bobject%20Object%5D',
            # 'city': 'Binghamton',
            # 'state': 'NY',
            'zip': zip_code,
            'location': '%5Bobject%20Object%5D',
            'newSearch': 'false',
            'makeCode': make_code,
            'marketExtension': 'off',
            'mileage': max_mileage,
            'numRecords': '100',
            'searchRadius': '500',
            'sortBy': 'derivedpriceASC',
            'startYear': min_year,
            'endYear': max_year,
            'dma': '%5Bobject%20Object%5D',
            'channel': 'KBB',
            'relevanceConfig': 'default',
            'vhrProviders': 'EXPERIAN',
            'vhrProvider': 'EXPERIAN',
            'stats': 'year%2Cderivedprice'
        }
        return base_parameters

    def _get_min_price(self, listings: list) -> int:
        ''' Returns minPrice for next 10 pages of non-key scrapes.
        Scraping is done on pages that are sorted by price ascending.
        To scrape 10 pages at a time, we use a specific minPrice.
        '''
        prices = []
        for car in listings:
            price = car['pricingDetail']
            if isinstance(price, dict):
                prices.append(price['salePrice'])
            else:
                prices.append(price)
        prices = sorted(set(prices))
        if len(prices) < 2:
            return 1e12
        min_price = int(prices[-2]) + 1
        return min_price

    def _store_scrape(self) -> None:
        listings = self._all_listings
        listings = [self._flatten_kbb_car_new(car) for car in listings]
        connection = self._db_connection
        # getting data from existing table, or just make it here and now
        try:
            db_data = pd.read_sql_query(
                'SELECT * FROM kbb_listings_new', connection)
        except pd.errors.DatabaseError:
            print(f'Storing listings. {len(listings)=}')
            pd.DataFrame(listings).to_sql(name='kbb_listings_new',
                                          con=connection,
                                          index=False)
            return
        vins = db_data['vin']
        listings = [car for car in listings if car['vin'] not in vins]
        print(f'Storing listings. {len(listings)=}')
        df = pd.DataFrame(listings)
        df = pd.concat([db_data, df]).drop_duplicates(subset='vin')
        df.to_sql(name='kbb_listings_new',
                  con=connection,
                  if_exists='replace',
                  index=False)

    def _update_all_listings(self, new_listings: list) -> list:
        extended_listings = [*self._all_listings, *new_listings]
        unique_listings = []
        tracked_vins = set()
        for car in extended_listings:
            vin = car['vin']
            if vin in tracked_vins:
                continue
            unique_listings.append(car)
            tracked_vins.add(vin)
        # if we didn't add anything new, abort
        if len(self._all_listings) == len(unique_listings):
            raise ValueError('No new listings!!! Aborting!!')
        self._all_listings = unique_listings

    def run_scrape(self, max_cars=1e12) -> None:
        parameters = self._base_parameters.copy()
        page = 0

        # some double checking in the terminal
        print(f'Scraping with the following settings:')
        print(self._url)
        [print(key, val) for key, val in self._base_parameters.items()]
        the_input = input('Press enter to continue ...')
        if the_input:
            exit()
        try:
            with hrequests.Session() as session:
                while True and len(self._all_listings) < max_cars:
                    if page > 8:
                        parameters.pop('firstRecord')
                        min_price = self._get_min_price(self._all_listings)
                        parameters['minPrice'] = min_price
                        page = 0
                    else:
                        first_record = page * 100
                        parameters['firstRecord'] = first_record
                    listings = self._fetch_kbb_listings(session=session,
                                                        base_url=self._url,
                                                        parameters=parameters)
                    self._update_all_listings(listings)
                    page += 1
                if len(listings) == 0:
                    raise ValueError('No listings!!! Aaaahh!!')
        except Exception as e:
            self._store_scrape()
            raise e


if __name__ == '__main__':
    run_test()
