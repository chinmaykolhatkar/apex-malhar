from apex import *
app = StreamingApp("TestApp")
app.fromFolder("/user/chinmay/test").printStream().launchDAG()

