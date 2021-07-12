from datetime import datetime, timedelta

class Domain():
	def __init__(self,nam_domain,int_time_limit_between_requests):
		self.time_last_access = datetime(1970,1,1)
		self.nam_domain = nam_domain
		self.int_time_limit_between_requests  = int_time_limit_between_requests

	@property
	def time_since_last_access(self):
		return datetime.now()-self.time_last_access
    
	def accessed_now(self):
		self.time_last_access = datetime.now()
        
	def is_accessible(self):   
		if datetime.now() >= self.time_last_access + timedelta(seconds=self.int_time_limit_between_requests):
			return True
		else:
			return False

	def __hash__(self):
		return hash(self.nam_domain)

	def __eq__(self, domain):
		return domain == self.nam_domain

	def __str__(self):
		return self.nam_domain

	def __repr__(self):
		return str(self)