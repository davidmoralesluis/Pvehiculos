package pvehiculos;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.sql.*;

import javax.persistence.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws SQLException {
        /*
        create type  public.tipo_vehf as(
nomeveh varchar(20),
pf numeric
);

create table finalveh (
id numeric,
dni varchar(10),
nomec varchar(30),
vehf tipo_vehf,
primary key (id));

*/
        int iDe=0;
        String deEneI="";
        String codVeh=null;

        String marcaVeh=null;
        int precioVeh=0;
        int annoMat=0;

        String nomeCliente=null;
        String insertarFila=null;


        //--------------------- Open a database connection -------------------------------------
        EntityManagerFactory emf =
                //Persistence.createEntityManagerFactory("$objectdb/db/vehicli.odb");
                Persistence.createEntityManagerFactory("/home/dam2a/objectdb-2.8.8/db/vehicli.odb");
        EntityManager em = emf.createEntityManager();
        em.getTransaction().begin();

        //----------------------- Creating a Mongo client --------------------------------------
        MongoClient mongo = new MongoClient("localhost",27017);

        // Accessing the database
        MongoDatabase test = mongo.getDatabase("test");

        // Retrieving a collection
        MongoCollection<Document> collection = test.getCollection("vendas");
        System.out.println("Collection sampleCollection selected successfully");

        //--------------------- Establecer Connection con postgreSQL -----------------------------
        Connection conn;
        String driver = "jdbc:postgresql:";
        String host = "//localhost:"; // tamen poderia ser una ip como "192.168.1.14"
        String porto = "5432";
        String sid = "postgres";
        String usuario = "dam2a";
        String password = "castelao";
        String url = driver + host+ porto + "/" + sid;
        try {
            conn = DriverManager.getConnection(url,usuario,password);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        //------------------------------------------------------------------------------------------------



        // FindIterable<Document> documents =  collection.find(Filters.eq("dni","361a"));
        FindIterable<Document> documents =  collection.find();

        for (Document document : documents) {
            iDe = document.getInteger("_id");
            deEneI = document.getString("dni");
            codVeh = document.getString("codveh");

            TypedQuery<Vehiculos> queryX = em.createQuery("select x from Vehiculos x", Vehiculos.class);


            TypedQuery<Vehiculos> queryV = em.createQuery("SELECT p FROM Vehiculos p where p.codveh='"+codVeh+"'" , Vehiculos.class);
            List<Vehiculos> resultsV = queryV.getResultList();
            for (Vehiculos p : resultsV) {
                marcaVeh=p.nomveh;
                precioVeh=p.prezoorixe;
                annoMat=p.anomatricula;

            }
            System.out.println();

            TypedQuery<Clientes> queryC = em.createQuery("SELECT c from Clientes c",Clientes.class);
            //TypedQuery<Clientes> queryC = em.createQuery("SELECT c FROM Clientes c where c.dni='"+deEneI+"'" , Clientes.class);
            List<Clientes> resultsC = queryC.getResultList();
            for (Clientes c : resultsC) {
                nomeCliente = c.nomec;
            }
            System.out.println(iDe+" - "+deEneI+" - "+nomeCliente+" - "+marcaVeh+" - "+precioVeh);
            System.out.println();



            insertarFila = "INSERT INTO finalveh"+"(id, dni, nomec, vehf) VALUES"+"(?,?,?,(?,?));";

            PreparedStatement stmt = conn.prepareStatement(insertarFila);

            stmt.setInt(1,iDe);
            stmt.setString(2,deEneI);
            stmt.setString(3,nomeCliente);
            //stmt.setObject(4,arrayV);
            stmt.setString(4,marcaVeh);
            stmt.setInt(5,precioVeh);
            stmt.executeUpdate();
            stmt.close();


            System.out.println("Document successfully\n");
        }

        ResultSet rs = conn.createStatement().executeQuery("select (vehf).pf from finalveh");
        String s=null;
        int i=0;
        while(rs.next()){
            System.out.println("rs IN");
           // s=rs.getString(1);
            i= rs.getInt(1);
            System.out.println(s+"--"+i);
            System.out.println("rs OUT");
        }
        conn.close();
        mongo.close();
        em.close();
        emf.close();

    }
}