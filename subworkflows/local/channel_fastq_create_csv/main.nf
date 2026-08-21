workflow CHANNEL_FASTQ_CREATE_CSV {
    take:
    ch_collected_meta // channel: collected list of meta maps
    pipeline // val: pipeline name
    strandedness // val: strandedness
    outdir // val: output directory

    main:
    ch_samplesheet = ch_collected_meta
        .flatten()
        .map { meta -> buildSamplesheetMeta(meta, pipeline, strandedness) }
        .toList()
        .map { items ->
            // Compute union of all keys across all rows
            def allKeys = [] as LinkedHashSet
            items.each { item -> allKeys.addAll(item.keySet()) }

            // Build header and rows with all columns
            def header = allKeys.collect { key -> '"' + key + '"' }.join(",")
            def rows = items.collect { meta ->
                allKeys
                    .collect { key ->
                        meta.containsKey(key) ? '"' + meta[key] + '"' : '""'
                    }
                    .join(",")
            }

            return "${header}\n${rows.join("\n")}"
        }
        // NOTE: collectFile with storeDir does not overwrite existing files.
        // If the pipeline is re-run with different input data, the stale
        // samplesheet from the previous run will remain in the storeDir.
        .collectFile(name: "${pipeline}_samplesheet.csv", sort: false, storeDir: "${outdir}/samplesheet")

    emit:
    samplesheet = ch_samplesheet
}

//
// FUNCTIONS
//

def buildSamplesheetMeta(meta, pipeline, strandedness) {
    def pipeline_extras = [
        atacseq: [replicate: 1],
        methylseq: [genome: ''],
        rnaseq: [strandedness: strandedness ?: ''],
        sarek: [patient: '', lane: "${meta.lane}"],
        seqinspector: [rundir: '', tags: '', flowcell: meta.fcid ?: ''],
        taxprofiler: [fasta: ''],
    ]

    def fastq_2 = (!meta.single_end && meta.fastq_2)
        ? [fastq_2: meta.publish_dir + '/' + file(meta.fastq_2).fileName]
        : [:]

    return [
        sample: meta.samplename,
        fastq_1: meta.publish_dir + '/' + file(meta.fastq_1).fileName,
    ] + fastq_2 + (pipeline_extras[pipeline] ?: [:])
}
