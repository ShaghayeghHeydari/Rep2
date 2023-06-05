from mrjob.job import MRJob
from mrjob.step import MRStep

class Duration(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
            reducer=self.reducer1)
        ]
    def mapper1(self,_,line):
        (TripDuration,gender,UKN)=line.split(',')
        yield gender, int(TripDuration)
    
    def reducer1(self,gender,duration):
        list1= list()
        for i in duration:
            list1.append(i)
        gender_map={'0':'unknown','1':'male','2':'female'}
        yield gender_map[gender],(sum(list1)/len(list1))/60
        
if __name__=='__main__':
    Duration.run()