class Cluster():
    def __init__(self, window, keystr, algorithm):
        self.keystr = keystr
        self.window = window
        self.algorithm = algorithm
        self.wordlist = []
        self.silhouettes = []
        self.totalscore = None
        self.stemmedDict = defaultdict(list)

