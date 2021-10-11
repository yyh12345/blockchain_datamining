import pandas as pd
import os
import json
import codecs

#获取区块边的信息
def dealJson():
    baseurl = '/Users/hejiajin1/PycharmProjects/交易模式/2020-12测试数据集/sourcedata/'
    outbaseurl = './data/'
    outaddressurl='./datatag/'
    outcsvlist = os.listdir(outbaseurl)
    addresslist = os.listdir(baseurl)

    for address in addresslist:
        filename = address[:-5] + '.csv'
        if "edge"+filename in outcsvlist:
            print("已经爬取")
            continue
        with codecs.open(baseurl+address, 'r', encoding='utf-8', errors='ignore') as txFile:
            jsonbytes = txFile.read()
            try:
                jsondatalist = json.loads(jsonbytes)
                if None in jsondatalist:
                    print('为空值')
                    continue
                for jsondata in jsondatalist:
                    vinlist = []
                    voutlist = []
                    vinaddresslist=[]
                    voutaddresslist=[]
                    inputs = jsondata['vin']
                    out = jsondata['vout']
                    for inaddress in inputs:
                        if (inaddress['addresses'] == None):
                            vinlist.append(None)
                        else:
                            addressjson = inaddress['addresses']
                            vinaddr = addressjson[0]['address']

                            if (vinaddr == None):
                                vinlist.append(None)
                            else:
                                remark = 'unknown'
                                if 'remark' in addressjson[0].keys():
                                    remark = addressjson[0]['remark']
                                vinlist.append(vinaddr)
                                vinaddresslist.append((vinaddr,remark))
                    # print(vinlist)
                    for outaddress in out:
                        if (outaddress['addresses'] == None):
                            voutlist.append(None)
                        else:
                            addressjson = outaddress['addresses']
                            voutaddr = addressjson[0]['address']
                            if (voutaddr == None):
                                voutlist.append(None)
                            else:
                                remark = 'unknown'
                                if 'remark' in addressjson[0].keys():
                                    remark = addressjson[0]['remark']
                                voutlist.append(voutaddr)
                                voutaddresslist.append((voutaddr,remark))
                    # print(voutlist)
                    getAddressSet(vinaddresslist,voutaddresslist,outaddressurl+'address'+filename)
                    # arrange_Tocsv(vinlist,voutlist,outbaseurl+'edge'+filename)
                    vinlist.clear()
                    voutlist.clear()
                    vinaddresslist.clear()
                    voutaddresslist.clear()
            except Exception as e:
                txFile.close()
                print(e)
        txFile.close()

def getAddressSet(alist, blist,filepath):
    addressset = []
    for adata in alist:
        addressset.append(adata)
    for bdata in blist:
        addressset.append(bdata)
    df = pd.DataFrame(addressset)
    df.to_csv(filepath, mode='a',header=False,index=False)
    addressset.clear()
def arrange_Tocsv(alist, blist, filepath):
    name = ['source', 'target']
    csvlist = []
    for adata in alist:
        for bdata in blist:
            csvlist.append((adata, bdata))
        if len(csvlist) >= 1000:
            datalist = pd.DataFrame(columns=name, data=csvlist)
            datalist.to_csv(filepath, mode='a', header=False, index=False)
            csvlist.clear()
    if len(csvlist) < 1000:
        datalist = pd.DataFrame(columns=name, data=csvlist)
        datalist.to_csv(filepath, mode='a', header=False, index=False)
        csvlist.clear()
def test():
    baseurl = 'I:/ABlockChainData/区块链测试数据集/2020-12/659364.json'
    hunbiurl='I:/混币/BitcoinFog/BitcoinFog/BitcoinFog/1A5cbwFqo13BEuWi64gk17hTGUn4N6FbMG.json'
    df = pd.read_json(hunbiurl,encoding='utf-8')
    print(df[0:1]['vout'][0])


if __name__ == '__main__':
    dealJson()
