from amazon_asin_ranking.spiders.mysql_manage import db


class CategoryURL(db.Model):
    __tablename__ = "CategoryURL"

    id = db.Column(db.Integer(), primary_key=True)
    category = db.Column(db.String(50))
    url = db.Column(db.String(500))
    status = db.Column(db.Integer())
    total = db.Column(db.Integer())
    subCategory = db.Column(db.String(500))

    def __init__(self,
                 category,
                 url,
                 status,
                 total,
                 subCategory
                 ):
        self.category = category
        self.url = url
        self.status = status
        self.total = total
        self.subCategory = subCategory

    def serialize(self):
        return {
            'url': self.url,
            'category': self.category,
            'status': self.status,
            'total': self.total,
            'subCategory': self.subCategory
        }


class Listing(db.Model):
    __tablename__ = "Listing"

    id = db.Column(db.Integer(), primary_key=True)
    asin = db.Column(db.String(100))
    isbn_10 = db.Column(db.String(100))
    ranking = db.Column(db.String(50))

    def __init__(self,
                 asin,
                 isbn_10,
                 ranking
                 ):
        self.asin = asin
        self.isbn_10 = isbn_10
        self.ranking = ranking

    def serialize(self):
        return {
            'asin': self.asin,
            'isbn_10': self.isbn_10,
            'ranking': self.ranking
        }
