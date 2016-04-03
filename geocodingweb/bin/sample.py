import web
from geocoding_data_tests import GeoCodingTest

urls = (
  '/geocoding', 'Index'
)

app = web.application(urls, globals())

render = web.template.render('templates/')

class Index(object):
    def GET(self):
    	geocoding = GeoCodingTest()
        test_results = geocoding.test_invalidShape()
        return render.index(greeting = "test_results")

if __name__ == "__main__":
    app.run()