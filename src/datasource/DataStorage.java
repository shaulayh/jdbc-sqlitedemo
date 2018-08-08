package datasource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataStorage {
    private static final String Database = "music.db";
    private static final String ARTIST_TABLE = "artists";
    public static final String artistName = "name";
    private static final String artist_id = "_id";
    private static final int indexId = 1;
    private static final int indexName = 2;

    public static final String QUERY_ARTIST = "select artists._id from artists where artists.name=?";
    public static final String QUERY_ALBUM = "select albums._id from albums where albums.name=?";
    public static final String QUERY_SONG = "select songs._id from songs where songs.title=? and songs.album=?";

    public static final String INSERT_ARTIST = "INSERT INTO artists(name) values(?)";
    public static final String INSERT_ALBUM = "INSERT INTO albums" + "(name,artist)" + "VALUES (?,?)";
    public static final String INSERT_SONG = "INSERT INTO songs" + "(track,title,album)" + "VALUES(?,?,?)";

    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    private PreparedStatement querySong;

    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;

    private Connection connection;
    private Statement statement;


    public void open() throws SQLException {
        connection = DriverManager.
                getConnection("jdbc:sqlite:C:\\Users\\shaul\\eclipse-workspace\\JavaSQL-learning\\" + Database);
        insertIntoArtists = connection.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
        insertIntoAlbums = connection.prepareStatement(INSERT_ALBUM, Statement.RETURN_GENERATED_KEYS);
        insertIntoSongs = connection.prepareStatement(INSERT_SONG);

        queryArtist = connection.prepareStatement(QUERY_ARTIST);
        queryAlbum = connection.prepareStatement(QUERY_ALBUM);
        querySong = connection.prepareStatement(QUERY_SONG);


    }

    public void close() throws SQLException {
        if (querySong != null) {
            querySong.close();
        }
        if (queryAlbum != null) {
            queryAlbum.close();
        }
        if (queryAlbum != null) {
            queryAlbum.close();
        }
        if (insertIntoArtists != null) {
            insertIntoArtists.close();
        }
        if (insertIntoAlbums != null) {
            insertIntoAlbums.close();
        }
        if (insertIntoSongs != null) {
            insertIntoSongs.close();
        }
        statement.close();
        connection.close();
    }

    public List<Artist> artistQuery() throws SQLException {

        StringBuilder queryString = new StringBuilder("SELECT * FROM ");
        queryString.append(ARTIST_TABLE).append(" ORDER BY ").append(artist_id);
        queryString.append(" COLLATE NOCASE");
        statement = connection.createStatement();
        List<Artist> artists = new ArrayList<>();
        try {
            ResultSet resultSet = statement.executeQuery(queryString.toString());

            while (resultSet.next()) {
                Artist artist = new Artist();
                artist.setId(resultSet.getInt(indexId));
                artist.setName(resultSet.getString(indexName));
                artists.add(artist);
            }
            return artists;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return artists;
    }


    public List<Album> albumQuery(String artistName) throws SQLException {
        ResultSet result = null;
        statement = connection.createStatement();
        List<Album> albums = new ArrayList<>();
        result = statement
                .executeQuery("select albums.name  from albums " +
                        "join artists on albums.artist= artists._id " +
                        "where artists.name='" + artistName + "'");

        if (!result.next()) {
            System.out.println("no data");
        } else {
            do {
                Album album = new Album();
                album.setName(result.getString("name"));
                albums.add(album);
            } while (result.next());
        }

        return albums;
    }

    public Artist findArtistWithSong(String songName) {
        Artist artist = new Artist();
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select songs.title ,artists._id ,artists.name from songs\n" +
                    "join albums on songs.album=albums._id\n" +
                    "join artists on albums.artist=artists._id\n" +
                    "where songs.title='" + songName + "'");
            if (resultSet.next()) {
                artist.setName(resultSet.getString(3));
                artist.setId(resultSet.getInt(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return artist;
    }

    public List<Song> songsForArtist(String artistName) throws SQLException {
        statement = connection.createStatement();
        List<Song> songs = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery("select artists.name,songs._id,songs.title,songs.track,songs.album from songs\n" +
                "join albums on songs.album=albums._id\n" +
                "join artists on albums.artist=artists._id\n" +
                "where artists.name='" + artistName + "'" +
                "ORDER BY songs.track");
        while (resultSet.next()) {
            Song song = new Song();
            song.setAlbum_id(resultSet.getInt(5));
            song.setId(resultSet.getInt(2));
            song.setTrack_id(resultSet.getInt(4));
            song.setName(resultSet.getString(3));
            songs.add(song);
        }
        return songs;
    }

    private int insertArtist(String artistName) throws SQLException {
        queryArtist.setString(1, artistName);
        ResultSet resultSet = queryArtist.executeQuery();
        if (resultSet.next()) {
            System.out.println("Artist is in the database");
            return resultSet.getInt(1);
        } else {
            insertIntoArtists.setString(1, artistName);
            int affectRows = insertIntoArtists.executeUpdate();
            if (affectRows != 1) {
                throw new SQLDataException("more/less than one added");
            } else {
                System.out.println("one Artist is added");
            }
            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("error with insertion of artist");
            }
        }
    }

    private int insertAlbum(String albumName, String artistName) throws SQLException {
        queryAlbum.setString(1, albumName);

        ResultSet resultSet = queryAlbum.executeQuery();
        if (resultSet.next()) {
            System.out.println("Album is in the database");
            return resultSet.getInt(1);
        } else {
            insertIntoAlbums.setString(1, albumName);
            insertIntoAlbums.setInt(2, insertArtist(artistName));
            int affectRows = insertIntoAlbums.executeUpdate();
            if (affectRows != 1) {
                throw new SQLDataException("more/less than one added");
            } else {
                System.out.println("one Album is added");
            }
            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("error with insertion of artist");
            }
        }
    }

    public void insertSong(int track, String title, String artistName, String album) throws SQLException {

        try {
            connection.setAutoCommit(false);
            int album_id = insertAlbum(album, artistName);

            querySong.setString(1, title);
            querySong.setInt(2, album_id);
            ResultSet resultSet = querySong.executeQuery();
            if (resultSet.next()) {
                System.out.println("already existing song");
            } else {
                insertIntoSongs.setInt(3, album_id);
                insertIntoSongs.setString(2, title);
                insertIntoSongs.setInt(1, track);
                int affectRow = insertIntoSongs.executeUpdate();
                if (affectRow == 1) {
                    connection.commit();
                    System.out.println("success adding song");
                } else {
                    throw new SQLException("error adding new songs");
                }
            }
        } catch (SQLDataException e) {
            System.out.println("error with Song commit");
            try {
                connection.rollback();
            } catch (SQLDataException e1) {
                System.out.println("couldn't rollback" + e1);
            }
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e2) {
                System.out.println("couldn't reset autocommit" + e2);
            }
        }
    }
}
