#!/usr/bin/python

import sys, os, urllib, traceback, json
import tornado, tornado.web, tornado.options, tornado.httpserver, tornado.httpclient
import config
from utils.utils import *


server_addr = "http://" + config.server_addr + "?"
url_base    = "http://127.0.0.1:" + config.listen_port + "?"

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('rt' + os.path.sep + 'index.html')
        
class DomainStatHandler(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", None)
        if _jobtype is None:
            _jobtype = "rt"
        _type = "schedulestat"
        _subtype = "domain"
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type":_type,"subtype":_subtype,"jobtype":_jobtype}))
        body = response.body
        _dict = json.loads(body)
        _domain = json.loads(_dict["info"])
        if _jobtype == "dt":
            try:
                self.render('dt' + os.path.sep + 'domains.html', domains = _domain)
            except Exception, e:
                traceback.print_exc()
        else:
            try:
                self.render('rt' + os.path.sep + 'domains.html', domains = _domain)
            except Exception, e:
                traceback.print_exc()
        
class DomainStatHandler2(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", None)
        if _jobtype is None:
            _jobtype = "rt"
        _type = "schedulestat"
        _subtype = "domain"
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type":_type,"subtype":_subtype,"jobtype":_jobtype}))
        body = response.body
        _dict = json.loads(body)
        _domain = json.loads(_dict["info"])
        _domain_ = dict2dict(_domain)
        self.write(_domain_)
        
class DomainControlHandler(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", None)
        if _jobtype is None:
            _jobtype = "rt"
        _type = "schedulecmd"
        _query = self.get_arguments("query")[0]
        _domain = self.get_arguments("domain")[0]
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type": _type, "domain": _domain, "query": _query, "jobtype": _jobtype}))
        body = response.body
        self.write(json.loads(body))
        
class WorkerControlHandler(tornado.web.RequestHandler):
    def get(self):
        _type = self.get_arguments("")
        _query = self.get_argument("query")[0]
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type": _type, "query": _query}))
        body = response.body
        self.write(body)
        
class TaskStatHandler(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", None)
        if _jobtype is None:
            _jobtype = "rt"
        _type = "schedulestat"
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type":_type,"subtype":"domain"}))
        domain_ret = json.loads(response.body)
        domains = json.loads(domain_ret["info"]) 

        _domain = self.get_argument("domain", None)
        _time = self.get_argument("time", None)
        _curPage = self.get_argument("curPage", 1)
        _sizePerPage = self.get_argument("sizePerPage", 10)
        _totalPage = self.get_argument("totalPage", 0)
        _subtype = "task"
        units = {}
        recordsNum = 0
        if _domain is not None:
            if _time is not None and _time != '':
                _time = _time.replace("-", "")
                response = client.fetch(server_addr + \
                                urllib.urlencode({"type": _type, "domain": _domain, "subtype": _subtype, "time": _time, "jobtype": _jobtype}))
            else :
                response = client.fetch(server_addr + \
                                urllib.urlencode({"type": _type, "domain": _domain, "subtype": _subtype, "jobtype": _jobtype, "start": (int(_curPage) - 1) * _sizePerPage, "end":int(_curPage)*_sizePerPage}))
            _dict = json.loads(response.body)
            if _dict["retCode"] == "OK":
                _result = json.loads(_dict["info"])
                units = _result["units"]
                recordsNum = _result["recordsNum"]
        else:
            response = client.fetch(server_addr + \
                                urllib.urlencode({"type": _type, "subtype": _subtype, "jobtype": _jobtype, "start": (int(_curPage) - 1) * _sizePerPage, "end":int(_curPage)*_sizePerPage}))
            _dict = json.loads(response.body)
            if _dict["retCode"] == "OK":
                _result = json.loads(_dict["info"])
                units = _result["units"]
                recordsNum = _result["recordsNum"]
                
        if _jobtype == "dt":
            try:
                self.render('dt' + os.path.sep + 'tasks.html', units = units)
            except Exception, e:
                print e
                self.render_string("dt" + os.path.sep + "error.html", info = units)
        else:
            try:
                print recordsNum
                us = sortDict(units)
                if (len(us) > 0) :
                    _totalPage = int(recordsNum) / _sizePerPage + 1
                start = (int(_curPage) - 1) * _sizePerPage
                self.render('rt' + os.path.sep + 'tasks.html', domains = domains, units = us,
                        curdomain = _domain, curtime = _time, curPage = _curPage, totalPage = _totalPage)
            except Exception, e:
                print e
                self.render_string("rt" + os.path.sep + "error.html", info = units)
class UnitTaskHandler(tornado.web.RequestHandler):
    def get(self, data = None):
        _jobtype = self.get_argument("jobtype", None)
        if _jobtype is None:
            _jobtype = "rt"
        _type = "schedulestat"
        _subtype = "unit"
        _unitid = self.get_argument("unitid", None)
        _domain = self.get_argument("domain", None)
        _totalPage = self.get_argument("totalPage", 0)
        _curPage = self.get_argument("curPage", 1)
        _sizePerPage = self.get_argument("sizePerPage", 10)
        if _unitid is None and data is not None and "unitid" in data:
            _unitid = data["unitid"] 
        if _domain is None and data is not None and "domain" in data:
            _unitid = data["domain"] 
        client = tornado.httpclient.HTTPClient()
        if _domain is None:
            response = client.fetch(server_addr + \
                                    urllib.urlencode({"type": _type, "subtype": _subtype, "unitid": _unitid, "jobtype": _jobtype}))
        else:
            response = client.fetch(server_addr + \
                                    urllib.urlencode({"type": _type, "domain": _domain, "subtype": _subtype, "unitid": _unitid, "jobtype": _jobtype}))
        body = response.body
        _dict = json.loads(body)
        _task = json.loads(_dict["info"])
        if _jobtype == "dt":
            self.render("dt" + os.path.sep + "unittask.html", tasks = _task)
        else:
            if (len(_task) > 0):
                _task[_unitid] = sortRTTask(_task[_unitid])
                _totalPage = len(_task[_unitid]) / int(_sizePerPage) + 1
            start = int(_curPage) * int(_sizePerPage)
            self.render("rt" + os.path.sep + "unittask.html", domain = _domain, tasks = _task,
                         unitid = _unitid, totalPage = _totalPage, curPage = _curPage, sizePerPage = _sizePerPage)


class ReDoTaskHandler(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", None)
        if _jobtype is None:
            _jobtype = "rt"
        _type = "scheduleredotask"
        _query = self.get_argument("query")
        _domain = self.get_argument("domain")
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type": _type, "domain": _domain, "query": _query, "jobtype": _jobtype}))
        body = response.body
        _result = json.loads(body)
        self.write(_result)

class JobCancelHandler(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", None)
        if _jobtype is None:
            _jobtype = "rt"
        _type = self.get_argument("type", None)
        _query = self.get_argument("query", None)
        _cmd = self.get_argument("cmd", None)
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type": _type, "query": _query, "jobtype": _jobtype, "cmd": _cmd}))
        body = response.body
        _result = json.loads(body)
        self.write(_result)

class MarkTaskHandler(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", None)
        if _jobtype is None:
            _jobtype = "rt"
        _type = "schedulemarktaskdone"
        _query = self.get_argument("query")
        _domain = self.get_argument("domain")
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type": _type, "domain": _domain, "query": _query,  "jobtype": _jobtype}))
        body = response.body
        _result = json.loads(body)
        self.write(_result)
        
class WorkNodeHandler(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", None)
        if _jobtype is None:
            _jobtype = "rt"
        _type = "schedulestat"
        _subtype = "running"
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + urllib.urlencode({"type": _type, "subtype": _subtype, "jobtype": _jobtype}))
        body = response.body
        _result = json.loads(body)
        _domain = json.loads(_result["info"])
        sorted_domains = sorted([(k, len(_domain[k])) for k in _domain], key = lambda d:d[1], reverse = True)
        if _jobtype == "dt":
            try:
                self.render("dt" + os.path.sep + "index.html", domains = [(k, _domain[k]) for (k, v) in sorted_domains])
            except Exception, e:
                traceback.print_exc()
                self.render("dt" + os.path.sep + "error.html", info = _domain)
        else:
            try:
                self.render("rt" + os.path.sep + "index.html", domains = [(k, _domain[k]) for (k, v) in sorted_domains])
            except Exception, e:
                traceback.print_exc()
                #self.render("rt" + os.path.sep + "error.html", info = _domain)

class RunningTaskHandler(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", "rt")
        _type = "schedulestat"
        _subtype = "running"
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + urllib.urlencode({"type": _type, "subtype": _subtype}))
        body = response.body
        _result = json.loads(body)
        _domain = json.loads(_result["info"])
        if _jobtype == "dt":
            try:
                self.render("dt" + os.path.sep + "running.html", taskArray = dict2List3( _domain))
            except Exception, e:
                self.render("dt" + os.path.sep + "error.html", info = _domain)
        else:
            try:
                self.render("rt" + os.path.sep + "running.html", taskArray = dict2List3( _domain))
            except Exception, e:
                self.render("rt" + os.path.sep + "error.html", info = _domain)

class DisableWorkerHandler(tornado.web.RequestHandler):
    def get(self):
        _type = self.get_argument("type")
        _domain = self.get_argument("domain")
        _query = self.get_argument("query")
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type": _type, "query": _query, "domain": _domain}))
        body = response.body
        _result = json.loads(body)
        self.write(_result)
        
class GetTaskFormHandler(tornado.web.RequestHandler):
    def get(self):
        _type = "schedulestat"
        _subtype = "domain"
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type":_type,"subtype":_subtype}))
        body = response.body
        _dict = json.loads(body)
        _domain = json.loads(_dict["info"])
        try:
            domainList = dict2List(_domain)
            self.render("rt" + os.path.sep + "sendtasks.html", domainDict = domainList, method = getMethod())
        except Exception, e:
            self.render("rt" + os.path.sep + "error.html", info = _domain)

class DoSendHandler(tornado.web.RequestHandler):
    def get(self):
        _type = self.get_argument("type", None)
        _query = self.get_argument("query", None)
        _jobtype = self.get_argument("jobtype", None)
        _trigger = self.get_argument("trigger", None)
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type":_type, "query":_query,"trigger":_trigger, "jobtype":_jobtype}))
        body = response.body
        _result = json.loads(body)
        self.write(_result)
        
    def post(self):
        _type = self.get_argument("type", None)
        _query = self.get_argument("query", None)
        _jobtype = self.get_argument("jobtype", None)
        _trigger = self.get_argument("trigger", None)
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type":_type, "query":_query,"trigger":_trigger, "jobtype":_jobtype}))
        body = response.body
        _result = json.loads(body)
        self.write(_result)
        
class SendTaskHandler(tornado.web.RequestHandler):
    def get(self):
        _jobtype = self.get_argument("jobtype", "rt")
        _type = "schedulestat"
        client = tornado.httpclient.HTTPClient()
        response = client.fetch(server_addr + \
                                urllib.urlencode({"type":_type,"subtype":"domain","jobtype":_jobtype}))
        domain_ret = json.loads(response.body)
        domains = json.loads(domain_ret["info"])
        if _jobtype == "dt":
            self.render("dt" + os.path.sep + "sendtask.html", domains = domains, curdomain = None)
        else:
            self.render("rt" + os.path.sep + "sendtask.html", domains = domains, curdomain = None)

class TmpHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("rt" + os.path.sep + "tmp.html")

class NavigationDTModule(tornado.web.UIModule):
    def render(self, activeTag):
        return self.render_string("dt" + os.path.sep + "navigation.html", activeTag =  activeTag)
    
class NavigationRTModule(tornado.web.UIModule):
    def render(self, activeTag):
        return self.render_string("rt" + os.path.sep + "navigation.html", activeTag =  activeTag)

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    tornado.options.parse_command_line()
    
    app = tornado.web.Application(
         handlers=[(r"/domainstat", DomainStatHandler), (r"/domaincontrol", DomainControlHandler),(r"/domainstat2", DomainStatHandler2),
                    (r"/taskstat", TaskStatHandler), (r"/unittask",UnitTaskHandler), (r"/redotask", ReDoTaskHandler),
                    (r"/disableworker", DisableWorkerHandler), (r"/runningtask", RunningTaskHandler), (r"/",WorkNodeHandler),
                    (r"/workNode",WorkNodeHandler), (r"/dosend", DoSendHandler), (r"/gettaskform", GetTaskFormHandler),
                    (r"/sendtask", SendTaskHandler), (r"/marktaskdone", MarkTaskHandler), (r"/jobcancel", JobCancelHandler),
                    (r"/tmp", TmpHandler),
                    ],
         template_path = os.path.join(os.path.dirname(__file__), config.template_path),
         static_path = os.path.join(os.path.dirname(__file__), config.static_path),
         ui_modules = {r"NavigationDT" : NavigationDTModule,
                       r"NavigationRT" : NavigationRTModule},
         debug=True
    )
    
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(config.listen_port)
    tornado.ioloop.IOLoop.instance().start()
