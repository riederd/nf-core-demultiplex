//
// Create pipeline samplesheet CSV from metadata using channel operators
//

workflow CHANNEL_FASTQ_CREATE_CSV {
    take:
    ch_collected_meta // channel: collected list of meta maps
    pipeline // val: pipeline name
    strandedness // val: strandedness
    outdir // val: output directory

    main:
    ch_samplesheet = ch_collected_meta
        .flatten()
        .map { meta -> buildPipelineMap(meta, pipeline, strandedness) }
        .map { pipeline_map ->
            def header = pipeline_map.keySet().collect { key -> '"' + key + '"' }.join(",")
            def values = pipeline_map.values().collect { value -> '"' + value + '"' }.join(",")
            return "${header}\n${values}"
        }
        .collectFile(name: "${pipeline}_samplesheet.csv", keepHeader: true, sort: true, storeDir: "${outdir}/samplesheet")

    emit:
    samplesheet = ch_samplesheet
}

//
// FUNCTIONS
//

def buildPipelineMap(meta, pipeline, strandedness) {
    def pipeline_map = [:]

    pipeline_map.sample = meta.samplename
    pipeline_map.fastq_1 = meta.publish_dir + '/' + file(meta.fastq_1).fileName

    if (!meta.single_end && meta.fastq_2) {
        pipeline_map.fastq_2 = meta.publish_dir + '/' + file(meta.fastq_2).fileName ?: ''
    }

    if (pipeline == 'rnaseq') {
        pipeline_map.strandedness = strandedness ?: ''
    }
    else if (pipeline == 'atacseq') {
        pipeline_map.replicate = 1
    }
    else if (pipeline == 'taxprofiler') {
        pipeline_map.fasta = ''
    }
    else if (pipeline == 'sarek') {
        pipeline_map.patient = ''
        pipeline_map.lane = "${meta.lane}"
    }
    else if (pipeline == 'methylseq') {
        pipeline_map.genome = ''
    }
    else if (pipeline == 'seqinspector') {
        pipeline_map.rundir = ''
        pipeline_map.tags = ''
        pipeline_map.flowcell = meta.fcid ?: ''
    }

    return pipeline_map
}
