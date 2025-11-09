#!/bin/bash
# submit_job.sh — создаёт sbatch-скрипт с подставленными переменными и отправляет его


# --- пользовательские переменные (редактировать перед запуском) ---
SAMPLE_NAME="7701"
SAMPLE_FINGERPRINT="12xy"
SAMPLE_ID="${SAMPLE_NAME}_${SAMPLE_FINGERPRINT}"
PIPELINE_NAME="basecalling_5mCG"
MODELS_DIR="/raid/kbajbekov/common_share/nanopore_service_files/dorado_models"
PORE_NAME="r941"
SAMPLE_MODELS_DIR="${MODELS_DIR}/${PORE_NAME}/"
GPUS_4_BASECALLING=8
MAIN_RES_DIR="/mnt/cephfs8_rw/nanopore2/processing"
PIPELINE_TIMEOUT="12:00:00"

TIMESTAMP=$(date +"%H-%M-%S_%d-%m-%Y")
J_NAME="${SAMPLE_NAME}_${PIPELINE_NAME}_${PORE_NAME}"
SAMPLE_DIR="${MAIN_RES_DIR}/${SAMPLE_NAME}/${SAMPLE_FINGERPRINT}"
JOB_WORK_DIR="${SAMPLE_DIR}/work"
JOB_RESULT_DIR="${SAMPLE_DIR}/result"
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

# Nextflow variables
export NXF_EXECUTOR=slurm
export NXF_OFFLINE=true
export NXF_WORK="${JOB_WORK_DIR}"
export NXF_LOG_FILE="${LOG_DIR}/nextflow/nxf_${J_NAME}.log"
export NXF_DATE_FORMAT="dd-MM-yyyy_HH:mm:ss"

# Pipeline variables
export GPUS_4_BASECALLING=${GPUS_4_BASECALLING}

nextflow run nxf-csp/ont-basecalling/ \
  --sample ${SAMPLE_ID} \
  --pore ${PORE_NAME} \
  --dorado_container nanoporetech/dorado:sha268dcb4cd02093e75cdc58821f8b93719c4255ed \
  --model_dir ${SAMPLE_MODELS_DIR} \
  --modifications 5mCG \
  --basecalling_model sup \
  --run_id ${J_NAME} \
  --input /raid/kbajbekov/common_share/github/nxf-csp/ont-basecalling/tests/data/full_size_fast5_dataset_1Tb.csv \
  --outdir ${JOB_RESULT_DIR} \
  -resume

EOF


echo "Submitting ${SBATCH_PATH} ..."
sbatch "${SBATCH_PATH}" || { echo "sbatch failed"; exit 1; }
