import argparse, os, gzip, json, tqdm, pickle
from dateutil import parser
from collections import defaultdict
from statistics import median


"""
Find long term reuse clusters that contain many smaller clusters
"""

class LongTermAnalyzer:

	def __init__(self, cluster_location, lt_threshold, sc_day_threshold, sc_count_threshold):
		self.cluster_location = cluster_location
		self.longterm_threshold = lt_threshold
		self.sc_day_threshold = sc_day_threshold
		self.sc_count_threshold = sc_count_threshold


	def find_clusters(self):
		"""
		Finds long term clusters that contain many smaller clusters
		"""
		files = os.listdir(self.cluster_location)
		clusters = []
		for filename in tqdm.tqdm(files, desc="Finding clusters..."):
			data = json.load(gzip.open(self.cluster_location + "/" + filename, "rt"))
			for cluster_key, cluster_data in data.items():
				if self.is_long_term(cluster_data):
					smaller_clusters = self.get_smaller_clusters(cluster_data)
					if len(smaller_clusters) > self.sc_count_threshold:
						gap_values = self.calculate_gap_values(smaller_clusters, cluster_data)
						clusters.append([filename, cluster_key, cluster_data])
						#print(smaller_clusters, gap_values)

		return clusters

	def is_long_term(self, cluster_data):
		"""
		Checks if cluster is a 'long term' cluster
		"""
		if cluster_data["span"] > self.longterm_threshold:
			return True
		else:
			return False


	def get_smaller_clusters(self, cluster_data):
		"""
		Checks if the cluster has smaller clusters inside
		"""
		sm = []
		hits = cluster_data["hits"]
		for i in range(len(hits)):
			curr = hits[i]
			curr_date = parser.parse(curr["date"])
			for j in range(i+1, len(hits)):
				comp = hits[j]
				comp_date = parser.parse(comp["date"])
				if (comp_date - curr_date).days > self.sc_day_threshold:
						break
				else:
					sm.append((i, j))

		clusters = self.extract_disjoint_clusters(sm)
		dates = []
		for c in clusters:
			dates.append([hits[i]["date"] for i in c])
		#dates = [hits[i]["date"] for c in clusters for i in c]
		#clusters = dates
		return clusters



	def extract_disjoint_clusters(self, sm):
		"""
		Extracts disjoint clusters from pairs of indices
		"""
		disjoint_clusters = []
		disjoint = defaultdict(list)
		for i, (start, end) in enumerate(sm):
			disjoint[start].append(i)
			disjoint[end].append(i)
		sets = []
		while len(disjoint) != 0:
			que = set(disjoint.popitem()[1])
			ind = set()
			while len(que) != 0:
				ind |= que
				que = set([y for i in que for x in sm[i] for y in disjoint.pop(x, [])]) - ind
			sets += [ind]

		for disjoint_indices_set in sets:
			disjoint_cluster = list(set([x for i in disjoint_indices_set for x in sm[i]]))
			disjoint_cluster.sort()
			disjoint_clusters.append(disjoint_cluster)

		return disjoint_clusters


	def calculate_gap_values(self, smaller_clusters, cluster_data):
		"""
		Calculate min, avg, median and max gap values
		"""
		gap_v = []
		for i in range(len(smaller_clusters)):
			start = smaller_clusters[i][-1]
			start_date = parser.parse(cluster_data["hits"][start]["date"])
			for j in range(i+1, len(smaller_clusters)):
				comp = smaller_clusters[j][0]
				comp_date = parser.parse(cluster_data["hits"][comp]["date"])
				gap = comp_date.year - start_date.year
				#gap = (comp_date - start_date).years
				gap_v.append(gap)

		return min(gap_v), sum(gap_v) / len(gap_v), median(gap_v), max(gap_v)





	def visualize(self, clusters):
		"""
		Visualize found clusters
		"""
		pass


if __name__ == "__main__":

	p = argparse.ArgumentParser(description="Find long term reuse clusters that contain many smaller clusters")
	p.add_argument("--cluster-location", required=True, help="Cluster folder")
	p.add_argument("--lt-threshold", required=False, default=10, type=int)
	p.add_argument("--sc-day-threshold", required=False, default=10, type=int)
	p.add_argument("--sc-count-threshold", required=False, default=2, type=int)

	args = p.parse_args()
	lta = LongTermAnalyzer(cluster_location=args.cluster_location, lt_threshold=args.lt_threshold, sc_day_threshold=args.sc_day_threshold, sc_count_threshold=args.sc_count_threshold)
	clusters = lta.find_clusters()
	pickle.dump(clusters, open("clusters.pkl", "wb"))
	lta.visualize(clusters)
