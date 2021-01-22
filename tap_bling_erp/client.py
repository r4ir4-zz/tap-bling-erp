import requests
import singer
import json

LOGGER = singer.get_logger()

class BlingERPClient():

    def __init__(self, config):
        self.config = config
        #self.headers = {'apikey':self.config['api_token']}
        #LOGGER.info(self.headers)

    def get_orders(self,start_date,end_date):
        api_data = []
       
        # define request parameters
        params = {'filters': 'dataAlteracao['+start_date+' TO '+end_date+']','apikey':self.config['api_token']}
        LOGGER.info(params)
        LOGGER.info("Start Date: {0}, Finish Date: {1}".format(start_date,end_date))
        
        response = [1]
        page = 1

        aux = True

        while aux == True: 

            url = "/".join([self.config['api_url'],'v2','pedidos','page='+str(page),'json'])         
            req = requests.get(url=url, params=params)
            response = req.json()
            response2 = json.dumps(response)

            if 'msg' in response2:
                aux = False
                LOGGER.info(aux)

            
            #LOGGER.info(response)   
            #for retorno in response:
           
            #LOGGER.info(response)
            LOGGER.info("Page requested: {}".format(page))
            LOGGER.info(params)
            api_data.extend(req.json())
            page = page + 1     
           

        return api_data

