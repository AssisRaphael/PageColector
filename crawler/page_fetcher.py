from bs4 import BeautifulSoup
from threading import Thread
import requests
from urllib.parse import urlparse,urljoin
from urllib import parse


class PageFetcher(Thread):
    def __init__(self, obj_scheduler):
        self.obj_scheduler = obj_scheduler




    def request_url(self,obj_url):
        """
            Faz a requisição e retorna o conteúdo em binário da URL passada como parametro

            obj_url: Instancia da classe ParseResult com a URL a ser requisitada.
        """        
        url = parse.urlunparse(obj_url)
        if "http" not in url:
            url = "http:" + url
        response = requests.get(url)
        response.headers['User-Agent'] = self.obj_scheduler.str_usr_agent
        if response.headers['content-type'].find('text/html') == -1:
            return None

        return response.content

    def discover_links(self,obj_url,int_depth,bin_str_content):
        """
        Retorna os links do conteúdo bin_str_content da página já requisitada obj_url
        """
        soup = BeautifulSoup(bin_str_content,features="lxml")
        for link in soup.select('a'):
            try:
                obj_new_url = urlparse(link['href'])
            except:
                continue
            
            if obj_new_url.netloc == '':
                if "http" in obj_new_url.path:
                    obj_new_url = urlparse(obj_new_url.path) 
                else:
                    obj_new_url = urlparse(urljoin(parse.urlunparse(obj_url), parse.urlunparse(obj_new_url)))
               # print('rrr: ', obj_new_url.netloc+obj_new_url.path)
                
            if obj_new_url.netloc != obj_url.netloc:
                int_new_depth = 1
            else:
                int_new_depth = int_depth + 1

            yield obj_new_url,int_new_depth

    def crawl_new_url(self):
        """
            Coleta uma nova URL, obtendo-a do escalonador
        """
        obj_url, int_depth = self.obj_scheduler.get_next_url()
        bin_str_content = self.request_url(obj_url)
        
        if bin_str_content is not None:
            #print(obj_url)
            multi_obj = self.discover_links(obj_url, int_depth, bin_str_content)
            while True:
                try:
                    url, depth = next(multi_obj)
                    #print(url)
                    self.obj_scheduler.add_new_page(url, depth)
                except StopIteration:
                    break
               

    def run(self):
        """
            Executa coleta enquanto houver páginas a serem coletadas
        """
        while not self.obj_scheduler.has_finished_crawl():
            self.crawl_new_url()
