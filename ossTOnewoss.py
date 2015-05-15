# -*- coding: utf-8 -*-
import re
from oss.oss_api import *
from oss.oss_xml_handler import *
import threading
import sys

class myOss(threading.Thread):
    '''得到 连接客户端 '''
    def __init__(self,ossUrl,keyId,keySecret,bucket,newKeyID,newKeySecret,newBucket,timePR,fileTree):
        threading.Thread.__init__(self)
        self.ossUrl = ossUrl
        self.keyId = keyId
        self.keySecret = keySecret
        self.bucket = bucket
        self.newKeyID = newKeyID
        self.newKeySecret = newKeySecret
        self.newBucket = newBucket
        self.timePR = timePR
        self.fileTree = fileTree

    def getOSSClient(self):
        return OssAPI(self.ossUrl, self.keyId, self.keySecret)
    
    def getNewOSSClient(self):
        return OssAPI(self.ossUrl,self.newKeyID,self.newKeySecret)
        


    '''得到所有目录树'''
    def getBucketFileList(self,oss,bucket,key):
        res = oss.get_bucket(bucket,marker=key,maxkeys='1000') # prefix=''  delimiter='/'
        if(res.status==200):
            redXml=res.read()
            objects_xml=GetBucketXml(redXml)
            result = objects_xml.list()
            #result = re.findall("<Key>(.*?)</Key>",redXml)
            newmarker = re.findall("<NextMarker>(.*?)</NextMarker>",redXml)
            return (result,newmarker)
        else:
            redFileMsg=open('errorMsg.csv','a')
            redFileMsg.write("redTreeERROR:%s,%s,%s\n" % (res.status,'',res.getheaders()))
    
    '''上传'''
    def OSSUpdate(self,oss,bucket,fileUrl,objFile):
        res = oss.put_object_from_fp( bucket, fileUrl, objFile )
        if(res.status==200):
            pass
            return res
        else:
            redFileMsg=open('errorMsg.csv','a')
            redFileMsg.write("putERROR:%s,%s,%s\n" % (res.status,fileUrl.encode('utf-8'),res.getheaders()))

    '''获取'''
    def OSSGetObj(self,oss,bucket,fileUrl):
        res = oss.get_object(bucket,fileUrl)
        if(res.status==200):
            pass
            return res
        else:
            redFileMsg=open('errorMsg.csv','a')
            redFileMsg.write("redERROR:%s,%s,%s\n" % (res.status,fileUrl.encode('utf-8'),res.getheaders()))

        
    def OssT0NewOss(self):
        
        oss=self.getOSSClient()
        newOss = self.getNewOSSClient()
        zs=0
        timePR=self.timePR
        msgFile=open('%s.txt'%(timePR),'a')
        #优化后
        for fileinfo in self.fileTree:
            fileUrl=fileinfo[0]
            uptime=fileinfo[1]

            if uptime.startswith(timePR):
                if not fileUrl.endswith('/'):
                    zs+=1
                    redFileobj=self.OSSGetObj(oss,self.bucket,fileUrl)
                    fp=StringIO.StringIO(redFileobj.read())
                    self.OSSUpdate(newOss,self.newBucket,fileUrl,fp)
                    msgFile.write('第%s次传输成功,url:%s\n'%(zs,fileUrl.encode('utf-8') ))
                else:
                    redFileMsg=open('mulu.csv','a')
                    redFileMsg.write('%s\n'%(fileUrl.encode('utf-8')))
        msgFile.write(timePR+'end')
        print timePR+'结束'
        
    def run(self):
        self.OssT0NewOss()

    
'''得到所有目录树'''
def getBucketFileList(oss,bucket,key):
    res = oss.get_bucket(bucket,marker=key,maxkeys='1000') # prefix=''  delimiter='/'
    if(res.status==200):
        redXml=res.read()
        objects_xml=GetBucketXml(redXml)
        result = objects_xml.list()
        #result = re.findall("<Key>(.*?)</Key>",redXml)
        newmarker = re.findall("<NextMarker>(.*?)</NextMarker>",redXml)
        return (result,newmarker)
    else:
        redFileMsg=open('errorMsg.csv','a')
        redFileMsg.write("redTreeERROR:%s,%s,%s\n" % (res.status,'',res.getheaders()))
    
def getMuLu(oss,bucket):
    marker=''
    infoList=[]
    while True:
        rqf=getBucketFileList(oss,bucket,marker)
        infoList+=rqf[0][0]
        nextMarker=rqf[1]
        if nextMarker==[]: break
        marker=nextMarker[0]
    return infoList

def getOSSClient(keyId,keySecret):
    ossUrl='oss.aliyuncs.com'
    return OssAPI(ossUrl, keyId, keySecret)

def test():
    #url = "oss-cn-hangzhou.aliyuncs.com"
    url = "oss.aliyuncs.com"
    keyId=''
    keySecret=''
    bucket=''

    newKeyId=''
    newKeySecret=''
    newBucket=''
    

    oss=getOSSClient(keyId,keySecret)
    flieTree=getMuLu(oss,bucket)
    #print flieTree

    #timePRlist=['2014-06','2014-07','2014-08']
    #timePRlist=['2014-09','2014-10','2014-11','2014-12']
    #timePRlist=['2015-01','2015-02','2015-03']
    timePRlist=['2015-04','2015-05']
    
    for da in timePRlist:
        OSS=myOss(url,keyId,keySecret,bucket,newKeyId,newKeySecret,newBucket,da,flieTree)      
        OSS.start()

if __name__ == '__main__':
    test()


