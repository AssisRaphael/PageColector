from urllib import robotparser, parse
from util.threads import synchronized
from collections import OrderedDict
from .domain import Domain
from time import sleep
from urllib.parse import urlparse

class Scheduler():
    #tempo (em segundos) entre as requisições
    TIME_LIMIT_BETWEEN_REQUESTS = 5

    def __init__(self,str_usr_agent,int_page_limit,int_depth_limit,arr_urls_seeds):
        """
            Inicializa o escalonador. Atributos:
                - `str_usr_agent`: Nome do `User agent`. Usualmente, é o nome do navegador, em nosso caso,  será o nome do coletor (usualmente, terminado em `bot`)
                - `int_page_limit`: Número de páginas a serem coletadas
                - `int_depth_limit`: Profundidade máxima a ser coletada
                - `int_page_count`: Quantidade de página já coletada
                - `dic_url_per_domain`: Fila de URLs por domínio (explicado anteriormente)
                - `set_discovered_urls`: Conjunto de URLs descobertas, ou seja, que foi extraída em algum HTML e já adicionadas na fila - mesmo se já ela foi retirada da fila. A URL armazenada deve ser uma string.
                - `dic_robots_per_domain`: Dicionário armazenando, para cada domínio, o objeto representando as regras obtidas no `robots.txt`
        """
        self.str_usr_agent = str_usr_agent
        self.int_page_limit = int_page_limit
        self.int_depth_limit = int_depth_limit
        self.int_page_count = 0

        self.dic_url_per_domain = OrderedDict()
        self.set_discovered_urls = set()
        self.dic_robots_per_domain = {}
        
        for seed in arr_urls_seeds:
            domain = Domain(seed, self.TIME_LIMIT_BETWEEN_REQUESTS)
            self.dic_url_per_domain[domain] = []


    @synchronized
    def count_fetched_page(self):
        """
            Contabiliza o número de paginas já coletadas
        """
   
        self.int_page_count += 1
        
        
    @synchronized
    def has_finished_crawl(self):
        """
            Verifica se finalizou a coleta
        """
        if(self.int_page_count > self.int_page_limit):
            return True
        return False


    @synchronized
    def can_add_page(self,obj_url,int_depth):
        """
            Retorna verdadeiro caso  profundade for menor que a maxima
            e a url não foi descoberta ainda
        """
        if int_depth > self.int_depth_limit:
            return False
        if str(obj_url) in self.set_discovered_urls:
            return False
        return True

    @synchronized
    def add_new_page(self,obj_url,int_depth):
        """
            Adiciona uma nova página
            obj_url: Objeto da classe ParseResult com a URL a ser adicionada
            int_depth: Profundidade na qual foi coletada essa URL
        """
        if not self.can_add_page(obj_url,int_depth):
            return False
        
        self.set_discovered_urls.add(str(obj_url))
        
        for domain in self.dic_url_per_domain.keys():
            if(obj_url.netloc == domain.nam_domain):
                self.dic_url_per_domain[domain].append((obj_url, int_depth))          
                return True
        
        name_domain = obj_url.netloc
        self.dic_url_per_domain[Domain(name_domain, self.TIME_LIMIT_BETWEEN_REQUESTS)] = [(obj_url, int_depth)]        
        return True
        #https://docs.python.org/3/library/urllib.parse.html

    @synchronized
    def get_next_url(self):
        """
        Obtem uma nova URL por meio da fila. Essa URL é removida da fila.
        Logo após, caso o servidor não tenha mais URLs, o mesmo também é removido.
        """
        domains = list(self.dic_url_per_domain.keys())
        
        while len(domains) > 0:
            for domain in domains:
                if len(self.dic_url_per_domain[domain]) > 0:
                    if domain.is_accessible():
                        url = self.dic_url_per_domain[domain].pop(0)
                        domain.accessed_now()
                        self.count_fetched_page()
                        return url
                else:
                    self.dic_url_per_domain.pop(domain)
                    #return (urlparse(domain.nam_domain),0)
            domains = list(self.dic_url_per_domain.keys())
            sleep(self.TIME_LIMIT_BETWEEN_REQUESTS)            
            
        return None, None
         

    def can_fetch_page(self,obj_url):
        """
        Verifica, por meio do robots.txt se uma determinada URL pode ser coletada
        """
        url = parse.urlunparse(obj_url)
        domain = Domain(obj_url.netloc, self.TIME_LIMIT_BETWEEN_REQUESTS)
        if domain in self.dic_robots_per_domain.keys():
            robot = self.dic_robots_per_domain[domain]
            return robot.can_fetch(self.str_usr_agent, url)

        robot =  robotparser.RobotFileParser()
        robot.set_url('http://' + obj_url.netloc + '/robots.txt')
        robot.read()       
        self.dic_robots_per_domain[domain] = robot
        return robot.can_fetch(self.str_usr_agent, url)
