from mrjob.job import MRJob
from mrjob.step import MRStep

class MaxStart(MRJob):
	def steps(self):
		return	[
				 MRStep(mapper= self.mapper1,
				 reducer=self.reducer1),
				 MRStep(mapper=self.mapper2,
				 reducer=self.reducer2)
		]

	def mapper1(self, _, line):
		(station, end, gender, _) = line.split(',')
		yield ((gender,station), 1)

	def reducer1(self,gender_station,count):
		yield (gender_station,sum(count))

	def mapper2(self,gender_station,count):
		gender, station = gender_station
		yield (gender, (station,count))

	def reducer2(self,gender,station_count):
		gender_map = {'0':'Male','1':'Female','2':'Unknown'}
		yield (gender_map[gender], max(station_count,key=lambda x: x[1]))


if __name__ == '__main__':
	MaxStart.run()
