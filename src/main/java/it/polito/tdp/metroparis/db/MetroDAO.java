package it.polito.tdp.metroparis.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.javadocmd.simplelatlng.LatLng;

import it.polito.tdp.metroparis.model.Connessione;
import it.polito.tdp.metroparis.model.CoppieF;
import it.polito.tdp.metroparis.model.Fermata;
import it.polito.tdp.metroparis.model.Linea;

public class MetroDAO {

	public List<Fermata> readFermate() {

		final String sql = "SELECT id_fermata, nome, coordx, coordy FROM fermata ORDER BY nome ASC";
		List<Fermata> fermate = new ArrayList<Fermata>();

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Fermata f = new Fermata(rs.getInt("id_Fermata"), rs.getString("nome"),
						new LatLng(rs.getDouble("coordx"), rs.getDouble("coordy")));
				fermate.add(f);
			}

			st.close();
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("Errore di connessione al Database.");
		}

		return fermate;
	}

	public List<Linea> readLinee() {
		final String sql = "SELECT id_linea, nome, velocita, intervallo FROM linea ORDER BY nome ASC";

		List<Linea> linee = new ArrayList<Linea>();

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Linea f = new Linea(rs.getInt("id_linea"), rs.getString("nome"), rs.getDouble("velocita"),
						rs.getDouble("intervallo"));
				linee.add(f);
			}

			st.close();
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("Errore di connessione al Database.");
		}

		return linee;
	}
	
	//METODO che permette di capire se due vertici sono connessi 
	//--> se il valore restituito dalla quesry e' uguale a zero allora il metodo restituisce false = non ci sono connessione altrimenti vero
	public boolean isConnesse(Fermata partenza, Fermata arrivo) {
		// TODO Auto-generated method stub
		
		String sql = "SELECT COUNT(*) AS C "
				+ "FROM connessione "
				+ "WHERE id_stazP = ? AND id_stazA = ?";
		
		
		try {
			Connection conn = DBConnect.getConnection();
			
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, partenza.getIdFermata());
			st.setInt(2, arrivo.getIdFermata());
			
			ResultSet rs = st.executeQuery();
			
			//rs ha un solo elemento quindi niente while
			rs.first();
			
			int c = rs.getInt("c");
			
			conn.close();
			
			return c!=0;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	
	}

	/*public List<Fermata> trovaCollegate(Fermata partenza) {
		// TODO Auto-generated method stub
		
		//creare una query piu' efficiente --> data una stazione di partenza, ottenere tutte le stazioni di arrivo
		//IN QUESTO MODO IL LAVORO VIENE FATTO TUTTO DAL DB
		
		
		//!!! ATTENZIONE : la query RESTITUISCE ID  e non l'oggetto fermata 
		//--> necessario una "CONVERSIONE"
		
		//2 MODI : 
		         //1)nel DB : fare un join tra tabella connessioni e fermata
		
		String sql = "SELECT * "
				+ "FROM fermata "
				+ "WHERE id_fermata IN ( "
				+ "SELECT id_stanzA "
				+ "FROM connessione "
				+ "WHERE id_stazP = ? "
				+ "GROUP BY id_stazA ) "
				+ "ORDER BY nome ASC"; 
		List<Fermata> fermate = new ArrayList<>();
		try  {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, partenza.getIdFermata());
		
			ResultSet res = st.executeQuery();
			while (res.next()) {
				Fermata f = new Fermata(res.getInt("id_fermata"), res.getString("nome"), new LatLng(res.getDouble("coordX"), res.getDouble("coordY")));
				fermate.add(f);
			}
			res.close();
			st.close();
			conn.close();
			
			return fermate;
			
		}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
	
		
		
		
		         

	}	*/
	
//2)in JAVA : trucco de IDENTITY MAP
	public List<Fermata> trovaIdCollegate(Fermata partenza, Map<Integer, Fermata> fermateIdMap) {
		
		String sql = "SELECT id_stazA "
				+ "FROM connessione "
				+ "WHERE id_stazP = ? "
				+ "GROUP BY id_stazA "; 
		
		List<Fermata> fermate = new ArrayList<>();
		try  {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, partenza.getIdFermata());
		
			ResultSet res = st.executeQuery();
			
			while (res.next()) {
				Integer idFermata = res.getInt("id_stazA");
				fermate.add(fermateIdMap.get(idFermata));
			}
			res.close();
			st.close();
			conn.close();
			
			return fermate;
			
		}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
	
	}
	
	public List<CoppieF> getAllCoppie(Map<Integer, Fermata> fermatIdMap) {
		
		String sql = "SELECT distinct id_stazP, id_stazA "
				+ "FROM connessione ";
		
		List<CoppieF> allCouple = new ArrayList<>();
		
		try {
			
			Connection conn = DBConnect.getConnection();
			
			PreparedStatement st = conn.prepareStatement(sql);
			

			ResultSet res = st.executeQuery();
			
			while (res.next()) {
				
				CoppieF coppia = new CoppieF(fermatIdMap.get(res.getInt("id_stazP")), fermatIdMap.get(res.getInt("id_stazA"))) ; 
				allCouple.add(coppia);
			}
			
			st.close();
			conn.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return allCouple;
		
	}

	

}
