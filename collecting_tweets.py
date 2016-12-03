from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import unicodecsv as csv
import re
import shutil
import os

csvFile = r'tweets.csv'
 
consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''
search_words = ''
lang = ''

#construct the handler and set the access token 
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)



#Listener class that defines what to do when a tweet is found
class MyListener(StreamListener):
 
    def on_data(self, data):
        try:
            tweet = json.loads(data)
            if tweet['lang'] == lang:
                print "Tweet em portugues!!!"
                
                #try to get the retweet information
                try:
                    id_original = tweet['retweeted_status']['id_str']
                    count_original = tweet['retweeted_status']['retweet_count']
                    if tweet['retweeted_status']['truncated']:
                        tweet_text = self.processTweet(tweet['retweeted_status']['extended_tweet']['full_text'])
                    else:
                        tweet_text = self.processTweet(tweet['retweeted_status']['text'])
                    self.incrementOnCsv(id_original,count_original,tweet_text)
                    
                #in case there is no retweet information, its an original tweet
                except KeyError:
                    text = tweet['text']
                    tweet_id = tweet['id']
                    tweet_text = self.processTweet(text)
                    
                    #if there is an 'RT' in the beggining
                    if self.isRetweet(text):
                        #try to find it on the file and increment
                        try:
                            self.incrementOnCsv(tweet_id,0,tweet_text,True)
                        #If the tweet is not found, add it as a new tweet
                        except:
                            self.addToCsv(tweet_id,0,tweet_text)
                            
                    #If its not an 'RT' tweet, just add it
                    else:        
                        self.addToCsv(tweet_id,0,tweet_text)
                
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True

    #This function adds a new tweet to the csv
    def addToCsv(self,tweet_id,count,text): 
        fields=[tweet_id,text.encode('utf-8'),str(count)]
        print fields
        with open(csvFile, 'a') as f:
            writer = csv.writer(f)
            writer.writerow(fields)

    #This function increment the counter of an existing tweet
    def incrementOnCsv(self,tweet_id,count,text,retweet = False):
        #if it is a retweet
        if retweet:
            try:
                #find the tweet and update
                line, line_number = self.findTweetTextOnCsv(text)
                line[2] = str(int(line[2]) + 1)
                self.updateCsv(line_number,line)
            except:
               self.addToCsv(tweet_id,count,text) 
        else:
            try:
                line, line_number = self.findTweetOnCsv(tweet_id)
                if int(line[2]) < count:
                    line[2] = str(count)
                    self.updateCsv(line_number,line)
            except:
                self.addToCsv(tweet_id,count,text)

    #This function finds the the register and the line number of a tweet based on the Text.
    def findTweetTextOnCsv(self,text):
        with open('tweets.csv', 'r') as f:
            reader = csv.reader(f, delimiter=',')
            for i,row in enumerate(reader):
                if text in row[1] or text == row[1] :
                    line = row
                    lineNumber = i
                    return (line, lineNumber)
                    
        return ()

    #This function finds the the register and the line number of a tweet based on the ID
    def findTweetOnCsv(self, tweet_id):
        with open('tweets.csv', 'rt') as f:
            reader = csv.reader(f, delimiter=',')
            for i,row in enumerate(reader):
                if tweet_id == row[0]:
                    line = row
                    lineNumber = i
                    return (line, lineNumber)
                    
        return ()

    #This function updates the CSV the parameters are the number of the line and the old register. These parameters are returned
    #by the functions findTweetOnCsv and findTweetTextOnCsv
    def updateCsv(self, line_number,line):
        print line
        tempfile = 'tmptweet.csv'
        with open(csvFile, 'rt') as f:
            with open (tempfile, 'wt') as ftemp:
                reader = csv.reader(f, delimiter=',')
                writer = csv.writer(ftemp, delimiter=',')

                for i,row in enumerate(reader):
                    if i == line_number:
                        writer.writerow(line)
                    else:
                        writer.writerow(row)
        os.remove(csvFile)
        shutil.move(tempfile, csvFile)

    #check if a tweet starts with 'RT'
    def isRetweet(self,text):
        if text.startswith('RT'):
            return True
    #This function removes links, user mentions and the 'RT' from the text.
    def processTweet(self,tweet_text):
        tokens = tweet_text.split(" ")
        for i,token in enumerate(tokens):
            if token.startswith('http'):
                tokens[i] = 'HTTPLINK'
            if token.startswith('@'):
                tokens[i] = '@USERNAME'
        if 'RT' in tokens:
            tokens.remove('RT')
        return " ".join(tokens)


twitter_stream = Stream(auth, MyListener())
twitter_stream.filter(track=[search_words])
