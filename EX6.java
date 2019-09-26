package advanced.transacoes;

import advanced.customwritable.FireAvgTempWritable;
import advanced.transacoes.writables.TransactionWeightAvgWritableKey;
import advanced.transacoes.writables.TransactionsWeightAvgWritable;
import advanced.transacoes.writables.TransactionsWeightValueAvgWritable;
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

public class EX6 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job j = new Job(c, "weight-avg");

        /*
         *  Registro de classe
         *  Qual é a classe principal que está rodando o main?
         *  -j.setJarByClass(WordCount.class);
         * */
        j.setJarByClass(EX6.class);
        j.setMapperClass(EX6.MapForTransactionWeightValueAvg.class);
        j.setReducerClass(EX6.ReduceForWeightValueAvg.class);
        j.setCombinerClass(EX6.CombineForWeightValueAvg.class);


        /*
         * definição dos tipos de saída
         *
         * */
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(TransactionsWeightValueAvgWritable.class);



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
    public static class MapForTransactionWeightValueAvg extends Mapper<LongWritable, Text, Text, TransactionsWeightValueAvgWritable> {

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

            //quebrando em palavras pelo caractere de ;
            String[] palavras = linha.split(";");

            //verificando se a primeira linha é o cabeçalho do csv,
            if(!palavras[1].equals("year") && !palavras[1].equals("") && !palavras[8].equals("") && !palavras[6].equals("")) {

                //verificando se o país é Brasil
                if (palavras[0].equals("Brazil")) {
                    /*
                    *  Chave: ano_commodity
                    *  Valor: preço, peso e n
                    * */
                    con.write(new Text(palavras[1]+"_"+palavras[3]), new TransactionsWeightValueAvgWritable(Float.parseFloat(palavras[5]), Float.parseFloat(palavras[6]), 1));
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
     *
     *
     * */
    public static class CombineForWeightValueAvg extends Reducer<Text, TransactionsWeightValueAvgWritable, Text, TransactionsWeightValueAvgWritable >{

        // Funcao de reduce
        public void reduce(Text key, Iterable<TransactionsWeightValueAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            float valor = 0;
            float peso = 0;
            int   n    = 0;

            //para cada item da lista de valores é feita uma soma
            for(TransactionsWeightValueAvgWritable v : values){
                //realizando a soma de valores,pesos e n's (ocorrencias) para calcular a merdia no reducer.
                valor += v.getValor();
                peso += v.getPeso();
                n    += v.getN();
            }


            //enviando o resultado para o reduce.
            con.write(key, new TransactionsWeightValueAvgWritable(valor, peso, n));
        }
    }

    public static class ReduceForWeightValueAvg extends Reducer<Text, TransactionsWeightValueAvgWritable, Text, Text >{

        // Funcao de reduce
        public void reduce(Text key, Iterable<TransactionsWeightValueAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            float valor = 0;
            float peso = 0;
            int   n    = 0;

            for(TransactionsWeightValueAvgWritable v : values){
                valor += v.getValor();
                peso += v.getPeso();
                n    += v.getN();
            }

            //calculo da média
            float valorFinal = (valor/peso)/n;


            //escrita no arquivo
            con.write(key, new Text(String.valueOf(valorFinal)));

        }
    }
}
