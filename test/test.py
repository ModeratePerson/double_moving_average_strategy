from tqsdk import TqApi, TqAuth
api = TqApi(auth=TqAuth("zyf_01", "@J8wrFVd5sHBcwF"))
quote = api.get_quote("SHFE.ni2206")
print (quote.last_price, quote.volume)