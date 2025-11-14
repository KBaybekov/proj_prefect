#!/bin/bash
# submit_job.sh — создаёт sbatch-скрипт с подставленными переменными и отправляет его


# --- пользовательские переменные (редактировать перед запуском) ---
SAMPLE_NAME="7777"
SAMPLE_FINGERPRINT="45gd"
SAMPLE_ID="${SAMPLE_NAME}_${SAMPLE_FINGERPRINT}"
PIPELINE_NAME="basecalling"
GPUS_4_BASECALLING=4
MAIN_RES_DIR="/raid/kbajbekov/common_share/github/proj_prefect/test_space/results"
MAIN_WORK_DIR="/raid/kbajbekov/common_share/github/proj_prefect/test_space/processing"
PIPELINE_TIMEOUT="3-00:00"

TIMESTAMP=$(date +"%H-%M-%S_%d-%m-%Y")
J_NAME="${SAMPLE_ID}_${PIPELINE_NAME}"
JOB_RESULT_DIR="${MAIN_RES_DIR}/${SAMPLE_NAME}/${SAMPLE_FINGERPRINT}/"
JOB_WORK_DIR="${MAIN_WORK_DIR}/${SAMPLE_NAME}/${SAMPLE_FINGERPRINT}/${PIPELINE_NAME}"
LOG_DIR="${JOB_RESULT_DIR}/logs"
SLURM_OUT="${LOG_DIR}/slurm/job_${J_NAME}.out"
SLURM_ERR="${LOG_DIR}/slurm/job_${J_NAME}.err"

mkdir -p "${JOB_WORK_DIR}" "${LOG_DIR}/slurm"

# --- сгенерируем sbatch-скрипт с уже подставленными значениями ---
SBATCH_PATH="${LOG_DIR}/slurm/job_${J_NAME}.sbatch"

cat > "${SBATCH_PATH}" <<EOF
#!/bin/bash
#SBATCH -J "slurm_${J_NAME}"
#SBATCH --chdir="${JOB_WORK_DIR}"
#SBATCH --requeue
#SBATCH --time=${PIPELINE_TIMEOUT}
#SBATCH --nodelist=vu10-2-027
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --partition=cpu_nodes
#SBATCH --output=${SLURM_OUT}
#SBATCH --error=${SLURM_ERR}
#SBATCH --wait

# Nextflow variables
export NXF_EXECUTOR=slurm
export NXF_OFFLINE=true
export NXF_WORK="${JOB_WORK_DIR}"
export NXF_LOG_FILE="${LOG_DIR}/nextflow/nxf_${J_NAME}.log"
export NXF_DATE_FORMAT="dd-MM-yyyy_HH:mm:ss"
export NXF_ANSI_LOG=false
export NXF_DISABLE_CHECK_LATEST=true
export NXF_OPTS=-name=nxf_${J_NAME}_${TIMESTAMP} resume=true

# Pipeline variables
export GPUS_4_BASECALLING=${GPUS_4_BASECALLING}

nextflow run nxf-csp/ont-basecalling/ \
  --sample ${SAMPLE_ID} \
  --run_id ${J_NAME} \
  --input /raid/kbajbekov/common_share/github/nxf-csp/ont-basecalling/tests/test_samplesheet.csv \
  --outdir ${JOB_RESULT_DIR} \

EOF


echo "Submitting ${SBATCH_PATH} ..."
sbatch "${SBATCH_PATH}" || { echo "sbatch failed"; exit 1; }
