import config
from mtgsdk import Card

def importCards():
    for keyword in config.keywords:
        cards = Card.where(text=keyword).all()

if __name__== "__main__":
    importCards
