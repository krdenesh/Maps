import web
from geocoding_data_tests import GeoCodingTest

urls = (
	'/', 'index',
  	'/geocoding/testinvalidShape/(.*)', 'testinvalidShape',
  	'/geocoding/testoverlappingpolygons/(.*)', 'testoverlappingpolygons',
  	'/geocoding/testpointinpolygon/(.*)', 'testpointinpolygon'
)

app = web.application(urls, globals())
render = web.template.render('templates/')

class index(object):
    def GET(self):
        return render.geocoding()

class testinvalidShape(object):
    def GET(self, test):
    	params  = web.input()
    	geocoding = GeoCodingTest(params)
        return geocoding.test_invalidShape()

class testoverlappingpolygons(object):
    def GET(self, test):
    	params  = web.input()
    	geocoding = GeoCodingTest(params)
        return geocoding.test_overlappingPolygons()

class testpointinpolygon(object):
    def GET(self, test):
    	params  = web.input()
    	geocoding = GeoCodingTest(params)
        return geocoding.test_PointInPolygon()

if __name__ == "__main__":
	app.run()