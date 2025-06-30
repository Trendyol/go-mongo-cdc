### Mongo Queries

```
use seller_contents_db

--delete
db.seller_contents.deleteOne({ _id: "1154522213:tr-TR" })

--insert
db.seller_contents.insertOne({
  _id: "1154522222434444:tr-TR",
  categoryId: 0,
  contentName: "Te-cl 18/2000 Liac Solo Akülü Ve Elektrikli Led Aydınlatma(AKÜ VE ŞARJ CİHAZI DAHİL DEĞİLDİR.) 21t",
  productGroupId: 78678,
  sellerContents: [
    {
      sellerId: 111,
      sellerName: "QC Test Tedarikçisi",
      sellerFollowerCount: 66,
      sellerScore: 0,
      rateAmount: 10,
      startDate: 1695859200,
      endDate: 1698537600,
      isInStock: true,
      isCoupon: false,
      isRushDelivery: false,
      lowestPriceDuration: 0,
      discountedPrice: 384,
      status: "IN_PROGRESS",
      advertId: "2f465d5d-4e96-47f6-9634-de98a02146b0",
      contentAdvertKey: "3999193010",
      eventTime: 1701092778425
    }
  ],
  lmd: 1746947701,
  socialProof: {
    hasReviewPhoto: false,
    favoriteCount: 0,
    averageRating: 0,
    totalCount: 0
  }
})

--update
db.seller_contents.updateOne(
  { _id: "1565454544:tr-TR" },
  { $set: { "socialProof.averageRating": 4.8 } }
)

```
