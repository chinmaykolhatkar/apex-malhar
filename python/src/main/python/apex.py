from py4j.java_gateway import JavaGateway

gateway = JavaGateway()
print "Gateway started"
sAppList = []

class StreamingApp(object):
  def __init__(self, name):
    self.context = gateway.entry_point.newStreamingApp(name)
    sAppList.append(self)
    
  def fromFolder(self, inputDir):
    self.context.fromFolder(inputDir)
    return self

  def printStream(self):
    self.context.printStream()
    return self

  def launchDAG(self):
    self.appId = self.context.launchDAG()
    print "AppID: " + self.appId + " started."

  def getAppId(self):
    return self.appId

  def kill(self):
    self.context.kill()


def getApp(appId):
  for s in sAppList:
    if s.getAppId() == appId:
      return s
  print "App Not Found"

def killApp(appId):
  a = getApp(appId)
  a.killApp()
