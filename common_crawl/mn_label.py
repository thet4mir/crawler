import fasttext

text = """
Бидэнд инфлюенсер биш эрүүл ухаан хэрэгтэй! 
Хууль үйлчилдэг, зарчим ярьдаг, үнэт зүйлээ дээдэлдэг Ерөнхийлөгчтэй, Парламенттай, Засгийн газартай, нийгэмтэй болохын төлөө та бид бөөндөө оролцох ёстой, тийм үү?
"""

lang_model = fasttext.load_model('lid.176.ftz')

print(lang_model)

prediction, confidence = lang_model.predict(text.strip().replace("\n", " "))

print(prediction, confidence)