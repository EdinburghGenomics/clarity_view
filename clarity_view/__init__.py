import flask as fl
import tornado
import tornado.wsgi
import tornado.httpserver
import tornado.ioloop
import tornado.autoreload

from clarity_view.queries import all_processes_per_project, sample_status_per_project
import genologics_sql

app = fl.Flask(__name__)

session = genologics_sql.utils.get_session()

@app.route('/')
def main_page():
    return fl.render_template('main_page.html')

@app.route('/project_status/')
def run_reports():
    return fl.render_template(
        'tables.html',
        table=sample_status_per_project(session)
    )





http_server = tornado.httpserver.HTTPServer(tornado.wsgi.WSGIContainer(app))
http_server.listen(5001)
tornado.ioloop.IOLoop.instance().start()

