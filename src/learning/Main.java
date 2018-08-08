package learning;

import datasource.Album;
import datasource.Artist;
import datasource.DataStorage;
import datasource.Song;

import java.sql.*;
import java.util.List;

public class Main {

    public static void main(String[] args) throws SQLException {
        DataStorage dataStorage = new DataStorage();
        String artistName = "WIZKID";
        String songName = "Evil Woman";
        dataStorage.open();
        List<Artist> artists;
        List<Album> albums;
        List<Song> songs;
        Artist artistFromSong;


        albums = dataStorage.albumQuery(artistName);
        artists = dataStorage.artistQuery();
        songs = dataStorage.songsForArtist(artistName);
        artistFromSong = dataStorage.findArtistWithSong(songName);

//      dataStorage.insertSong(7,"Rat Salad","Gafar","Paranoid");
//        for (Artist artist : artists) {
//            System.out.println(" " + artist.getId() + " " + artist.getName());
//        }

 //       dataStorage.insertAlbum("SFTOS", "WIZKID");

          dataStorage.insertSong(2,"Sexy","WIZKID","SFTOS");

        for (Album album : albums) {
            System.out.println("" + artistName + " " + album.getName());
        }


        dataStorage.close();
//
//        for (Album album : albums) {
//            System.out.println(album.getName());
//        }
//
//        System.out.println("___________________________________________");
//        System.out.println("Song belongs to " + artistFromSong.getName());
//        System.out.println("___________________________________________");
        for (Song song : songs) {
            System.out.println(song.getTrack_id() + " " + song.getName()+ " " +  song.getAlbum_id());
        }
    }
}

