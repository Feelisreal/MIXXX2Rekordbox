import sqlite3
import beats_pb2
import xml.etree.ElementTree as ET

# Function to calculate the beat position in seconds
def calculate_beat_position(blob_data, sample_rate):
    # Ensure blob_data is in bytes
    #if isinstance(blob_data, str):
       # blob_data = blob_data.encode('utf-8')
    
    beatgrid = beats_pb2.BeatGrid()
    beatgrid.ParseFromString(blob_data)

    if beatgrid.HasField("first_beat") and beatgrid.first_beat.HasField("frame_position"):
        frame_position = beatgrid.first_beat.frame_position
        beat_length = 60.0 / beatgrid.bpm.bpm

        position_seconds = frame_position / sample_rate

        if position_seconds < 0:
            position_seconds += beat_length

        if position_seconds > beat_length:
            position_seconds -= beat_length

        return position_seconds
    else:
        return None

# Connect to your SQLite DB
db_path = r'C:\Users\felix\AppData\Local\Mixxx\mixxxdb.sqlite'
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Your SQL query
query = """
SELECT
    T0.id as TrackID,
    IFNULL(T0.artist, '') as Artist,
    IFNULL(T0.title, '') as Name,
    IFNULL(T0.album, '') as Album,
    IFNULL(year, '') as Year,
    IFNULL(T0.genre, '') as Genre,
    IFNULL(T0.tracknumber, '') as TrackNumber,
    IFNULL(T0.comment, '') as Comments,
    IFNULL(T0.duration, '') as TotalTime,
    IFNULL(T0.samplerate, '') as SampleRate,
    IFNULL(T0.bitrate, '') as BitRate,
    IFNULL(T0.bpm, '') as AverageBpm,
    IFNULL(T0.timesplayed, '') as PlayCount,
    IFNULL(CASE WHEN T0.filetype = 'm4a' THEN 'M4A File' WHEN T0.filetype = 'mp3' THEN 'MP3 File'ELSE T0.Filetype END, '') as Kind,
    IFNULL(T0.key, '') as Tonality,
    IFNULL(T0.composer, '') as Composer,
    'file://localhost/' || T1.location as Location,
    IFNULL(T1.filesize, '') as Size,
    '0X' || printf('%02X%02X%02X', ((T0.Color >> 16) & 255), ((T0.Color >> 8) & 255), (T0.Color & 255)) AS HexColor,
	CASE 	WHEN T0.rating = 0 THEN 0 
			WHEN T0.Rating = 1 THEN 51
			WHEN T0.Rating = 2 THEN 102
			WHEN T0.Rating = 3 THEN 153
			WHEN T0.Rating = 4 THEN 204
			WHEN T0.Rating = 5 THEN 255
			WHEN T0.Rating > 5 THEN 255
			ELSE 0 
	END as Rating,
    CASE 
        WHEN T0.Color IS NULL THEN ''
        WHEN T0.Color = '86264' THEN '0x0000FF' --'Blue'
        WHEN T0.Color = '2023424' THEN '0x00FF00' --'Green'
        WHEN T0.Color = '8849664' THEN '0xFF0000' --'Red'
        WHEN T0.Color = '9963768' THEN '0x660099' --'Purple'
        WHEN T0.Color = '16281848' THEN '0xFF007F' --'Rose' 
        WHEN T0.Color = '16293936' THEN '0xFFA500' --'Orange'
        WHEN T0.Color = '16311089' THEN '0xFFFF00' --'Yellow'
    END as HexColor2,
    CASE 
        WHEN T0.Color IS NULL THEN ''
        WHEN T0.Color = '86264' THEN 'Blue'
        WHEN T0.Color = '2023424' THEN 'Green'
        WHEN T0.Color = '8849664' THEN 'Red'
        WHEN T0.Color = '9963768' THEN 'Purple'
        WHEN T0.Color = '16281848' THEN 'Pink' 
        WHEN T0.Color = '16293936' THEN 'Orange'
        WHEN T0.Color = '16311089' THEN 'Yellow'
    END as Grouping,
    T0.color,
    T0.beats
FROM library T0
INNER JOIN track_locations T1 ON T0.ID = T1.id
WHERE T0.mixxx_deleted = 0

"""

# Execute the SQL query
cursor.execute(query)

# Fetch data
data = cursor.fetchall()

# SQL query to retrieve position marks
position_marks_query = """
SELECT 	track_id as TrackID, 
		label as Name,
		0 as Type, 
        ROUND(position / (2.0 * T1.Samplerate), 3) as Start, 
		hotcue as Num,
		((T0.Color >> 16) & 255) AS Red,
		((T0.Color >> 8) & 255) AS Green,
		(T0.Color & 255) AS Blue
FROM cues T0 INNER JOIN library T1 ON T0.track_id = T1.id
WHERE T0.Type = 1 AND T1.mixxx_deleted = 0
"""

cursor.execute(position_marks_query)

# Fetch all position marks as dictionaries
position_marks_data = cursor.fetchall()
	
# Create a dictionary to store position marks by TrackID
position_marks_dict = {}

# Iterate through the position marks and group them by TrackID
for position_mark in position_marks_data:
    track_id = position_mark['TrackID']
    if track_id not in position_marks_dict:
        position_marks_dict[track_id] = []
    position_marks_dict[track_id].append(position_mark)
    
    
# Your SQL query for playlists and crates
playlists_crates_query = """
SELECT '1' || T0.ID AS id, T0.Name, T1.track_id, T1.position
FROM playlists T0 
INNER JOIN PlaylistTracks T1 ON T0.id = T1.playlist_id
INNER JOIN library T2 ON T1.track_id = T2.id
WHERE T0.hidden = 0 

UNION ALL

SELECT '2' || T0.ID AS id, T0.Name || '(Crate)', T1.track_id, T1.track_id as position
FROM crates T0 
INNER JOIN crate_tracks T1 ON T0.id = T1.crate_id
INNER JOIN library T2 ON T1.track_id = T2.id

ORDER BY id, position asc
"""

# Execute the SQL query for playlists and crates
cursor.execute(playlists_crates_query)

# Fetch playlists and crates data
playlists_crates_data = cursor.fetchall()

# Create a dictionary to store playlists and crates by ID
playlist_dict = {}

# Iterate through playlists and crates and group them by ID
for item in playlists_crates_data:
    playlist_id = item['id']
    if playlist_id not in playlist_dict:
        playlist_dict[playlist_id] = {'Name': item['Name'], 'Tracks': []}
    playlist_dict[playlist_id]['Tracks'].append(item['track_id'])
   

# Create the DJ_PLAYLISTS root element
dj_playlists = ET.Element("DJ_PLAYLISTS", Version="1.0.0")

# PRODUCT element
product = ET.SubElement(dj_playlists, "PRODUCT", Name="rekordbox", Version="6.7.7", Company="AlphaTheta")

# COLLECTION element
collection = ET.SubElement(dj_playlists, "COLLECTION", Entries=str(len(data)))

# Iterate through your data and create TRACK elements
for row in data:
    track = ET.SubElement(collection, "TRACK",
    TrackID=str(row['TrackID']),
    Name=str(row['Name']),
    Artist=str(row['Artist']),
    Composer=str(row['Composer']),
    Album=str(row['Album']),
    Grouping=str(row['Grouping']),
    Genre=str(row['Genre']),
    Kind=str(row['Kind']),
    Size=str(row['Size']),
    TotalTime=str(row['TotalTime']),
    DiscNumber="",
    TrackNumber=str(row['TrackNumber']),
    Year=str(row['Year']),
    AverageBpm=str(row['AverageBpm']),
    DateAdded="",
    BitRate=str(row['BitRate']),
    SampleRate=str(row['SampleRate']),
    Comments=str(row['Comments']),
    PlayCount=str(row['PlayCount']),
    Rating=str(row['Rating']),
    Location=str(row['Location']),
    Remixer="",
    Tonality=str(row['Tonality']),
    Label="",
    Mix="",
    Colour=str(row['HexColor2'])
)
# Calculate the beat position in seconds from the blob
    beats_blob = row['beats']
    sample_rate = row['SampleRate']
    if beats_blob is not None:
        beat_position_seconds = calculate_beat_position(beats_blob, sample_rate)

    if beat_position_seconds is not None:
        # TEMPO element
        tempo = ET.SubElement(track, "TEMPO", Inizio=f"{beat_position_seconds:.3f}", Bpm=str(row['AverageBpm']), Metro="4/4", Battito="1")


    # TEMPO element
    # tempo = ET.SubElement(track, "TEMPO", Inizio="0.085", Bpm=str(row['AverageBPM']), Metro="4/4", Battito="1")
	
	 # POSITION_MARK elements for the track
    if row['TrackID'] in position_marks_dict:
        for position_mark in position_marks_dict[row['TrackID']]:
            position_mark_elem = ET.SubElement(track, "POSITION_MARK", 
			    Name=str(position_mark['Name']),
                Type=str(position_mark['Type']),
                Start=str(position_mark['Start']),
                Num=str(position_mark['Num']),
                Red=str(position_mark['Red']),
                Green=str(position_mark['Green']),
                Blue=str(position_mark['Blue'])			
			)
# Create PLAYLISTS node under COLLECTION
playlists_node = ET.SubElement(dj_playlists, "PLAYLISTS")

# Create a dictionary to store playlists and their tracks
playlist_tracks_dict = {}

# Iterate through playlists and crates and group tracks by playlist
for item in playlists_crates_data:
    playlist_id = item['id']
    if playlist_id not in playlist_tracks_dict:
        playlist_tracks_dict[playlist_id] = {'Name': item['Name'], 'Tracks': []}
    playlist_tracks_dict[playlist_id]['Tracks'].append(item['track_id'])

# Create a ROOT node to hold all playlists
root_node = ET.SubElement(playlists_node, "NODE", Name="ROOT", Type="0", Count=str(len(playlist_tracks_dict)))

# Create NODE elements for each playlist/crate
for playlist_id, playlist_data in playlist_tracks_dict.items():
    playlist_tracks = playlist_data['Tracks']
    node = ET.SubElement(root_node, "NODE", Name=playlist_data['Name'], Type="1", KeyType="0", Entries=str(len(playlist_tracks)))

    # Create TRACK elements for tracks within each playlist/crate
    for track_id in playlist_tracks:
        track_node = ET.SubElement(node, "TRACK", Key=str(track_id))


# Create the XML file
output_xml_path = 'output.xml'
tree = ET.ElementTree(dj_playlists)
tree.write(output_xml_path, encoding="UTF-8", xml_declaration=True)

# Close the DB connection
conn.close()

print(f'Data extracted from {db_path} and written to {output_xml_path}')
