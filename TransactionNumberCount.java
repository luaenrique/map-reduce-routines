package advanced.transacoes;

import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransactionNumberCount {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job j = new Job(c, "transaction-numbercount");

        /*
         *  Registro de classe
         *  Qual é a classe principal que está rodando o main?
         *  -j.setJarByClass(WordCount.class);
         * */
        j.setJarByClass(TransactionNumberCount.class);
        j.setMapperClass(TransactionNumberCount.MapForTransactionNumber.class);


        /*
        *
        * Utilizando a mesma classe para combiner e reducer, pois ambas seguem o mesmo procedimento
        *
        * */
        j.setReducerClass(TransactionNumberCount.ReduceForTransactionNumber.class);
        j.setCombinerClass(TransactionNumberCount.ReduceForTransactionNumber.class);

        /*
         * definição dos tipos de saída
         *
         * */
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);



        /*
         * Definindo arquivos de entrada e saída
         *
         * */
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    /*
     * Map
     *
     * Mapper<Tipo da chave de entrada, tipo do valor de entrada,
     *         Tipo da chave de saída, Tipo do valor de saída>
     *   Mapper< id arquivo, texto do arquivo, chave, valor>
     *       se for arquivo de texto LongWritable e Text nunca mudam
     *
     * */
    public static class MapForTransactionNumber extends Mapper<LongWritable, Text, Text, LongWritable> {

        // Funcao de map
        /*
         * Value = cada linha
         *
         *
         * Context é responsável por enviar o map para o reduce
         * */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //capturando as linhas
            String linha = value.toString();

            //quebrando em palavras
            String[] palavras = linha.split(";");

            //verificando se a linha em questão é o cabeçalho do csv
            if(!palavras[8].equals("quantity") && !palavras[8].equals("") ) {
                //verificando se o país é o Brasil
                if (palavras[0].equals("Brazil")){
                    //passando os resultados para o reduce agrupados por mercadoria
                    con.write(new Text(palavras[3]), new LongWritable(Long.parseLong(palavras[8])));
                }
            }
        }
    }

    /*
     *
     * Reduce
     *
     * Reducer<Tipo da chave de entrada, tipo do valor de entrada -> saída do map!
     *           Tipo da chave de saída, tipo do valor de saída>
     *          <Palavra, ocorrencias,
     *            ...>
     *
     *
     * */
    public static class ReduceForTransactionNumber extends Reducer<Text, LongWritable, Text, LongWritable >{

        // Funcao de reduce
        public void reduce(Text commodity, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            //para cada item da lista de valores é feita uma soma
            for(LongWritable v : values){
                sum += v.get();
            }


            //apresentar o resultado final de acordo com a commodity
            con.write(commodity, new LongWritable(sum));
        }
    }

}
