import pandas as pd
import nltk
# students = [('jack', 34, 'Sydney', 155),
#             ('Riti', 31, 'Delhi', 177.5),
#             ('Aadi', 16, 'Mumbai', 81),
#             ('Mohit', 31, 'Delhi', 167),
#             ('Veena', 12, 'Delhi', 144),
#             ('Shaunak', 35, 'Mumbai', 135),
#             ('Shaun', 35, 'Colombo', 111)
#             ]
#
# # Create a DataFrame object
# data = pd.DataFrame(students, columns=['Name', 'Age', 'City', 'Score'])
# data = data.to_numpy().tolist()
# new_data = []
# for i in data:
#     replace_data = tuple(i)
#     new_data.append(replace_data)
# new_data = str(new_data)[1:-1]
# print new_data
# from nltk.corpus import stopwords
# stopwords.words('english')
# nltk.download()
# en_stops = stopwords.words('english')
# print en_stops
# from itertools import chain
# x = iter([1,2,3])      #Create Generator Object (listiterator)
# y = iter([3,4,5])      #another one
# result = chain(x, y)
# print result
import lib
file = open(lib.stopwords_, "r")
readline = file.read().splitlines()
print readline