import enum

class Model(enum.Enum):
    Additive = 0
    Dominant = 1
    Recessive = 2

class Format(enum.Enum):
    BGEN = 0

class Family(enum.Enum):
    Binomial = 0