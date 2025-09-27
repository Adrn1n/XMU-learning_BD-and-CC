#!/bin/bash

HDFS_CMD="$HADOOP_HOME/bin/hdfs dfs"

show_menu(){
cat<<EOF
0. Exit
1. Upload
2. Download
3. Read file
4. Inspect info
5. Create (dir end with '/')
6. Delete
7. Move
EOF
}

handleUpload(){
    if [[ (${3} -eq 1) ]] || (! (${HDFS_CMD} -test -e "${2}" 2>/dev/null));then
        ${HDFS_CMD} -test -d $(dirname "${2}") || ${HDFS_CMD} -mkdir -p $(dirname "${2}")
        ${HDFS_CMD} -put -f "${1}" "${2}"
    elif [[ ${3} -eq 0 ]];then
        ${HDFS_CMD} -appendToFile "${1}" "${2}"
    elif [[ ${3} -eq 2 ]];then
        (cat "${1}" <( ${HDFS_CMD} -cat "${2}" )) | (${HDFS_CMD} -put -f - "${2}")
    fi
}

handleDownload(){
    local tarPath="${2}"
    if [[ (${3} -ne 0) && (-e "${tarPath}") ]];then
        local name="${tarPath%%.*}"
        local extName="${tarPath#*.}"
        if [[ "${name}" == "${extName}" ]];then
            extName=""
        else
            extName=".$extName"
        fi
        local cnt=0
        while [[ -e "${name}_${cnt}${extName}" ]];do
            ((++cnt))
        done
        tarPath="${name}_${cnt}${extName}"
        echo "Local path exists, rename to ${tarPath}"
    fi
    ${HDFS_CMD} -get -f "${1}" "${tarPath}"
}

handleRead(){
    ${HDFS_CMD} -cat "${1}"
}

handleInspect(){
    if [[ ${2} -ne 0 ]];then
        ${HDFS_CMD} -ls -R "${1}"
    else
        ${HDFS_CMD} -ls "${1}"
    fi
}

handleCreate(){
    if [[ ${1: -1} == '/' ]];then
        ${HDFS_CMD} -mkdir -p "${1}"
    else
        ${HDFS_CMD} -test -d $(dirname "${1}") || ${HDFS_CMD} -mkdir -p $(dirname "${1}")
        ${HDFS_CMD} -touchz "${1}"
    fi
}

handleDelete(){
    if (${HDFS_CMD} -test -d "${1}" 2>/dev/null) && [[ $(${HDFS_CMD} -ls -R "$1" 2>/dev/null | wc -l) -gt 0 ]];then
        echo "Delete non-empty dir"
    fi
    ${HDFS_CMD} -rm -r "${1}"
}

handleMove(){
    ${HDFS_CMD} -test -d $(dirname "${2}") || ${HDFS_CMD} -mkdir -p $(dirname "${2}")
    ${HDFS_CMD} -mv "${1}" "${2}"
}

handleArgs(){
    case "${1}" in
        upload)
            handleUpload "${2}" "${3}" "${4}"
            ;;
        download)
            handleDownload "${2}" "${3}" "${4}"
            ;;
        read)
            handleRead "${2}"
            ;;
        inspect)
            handleInspect "${2}" "${3}"
            ;;
        create)
            handleCreate "${2}"
            ;;
        delete)
            handleDelete "${2}"
            ;;
        move)
            handleMove "${2}" "${3}"
            ;;
        *)
            echo "Unknown operation: ${1}"
            ;;
    esac
}

interactiveMode(){
    show_menu
    while :
    do
        read -p "Enter choice: " choice
        case "${choice}" in
            0)
                echo "Exit"
                break
                ;;
            1)
                read -p "local path: " localPath
                read -p "hdfs path: " hdfsPath
                read -p "append, overwrite or prepend (0/[1]/2): " mode
                if [[ ${mode} -ne 0 && ${mode} -ne 2 ]];then
                    mode=1
                fi
                handleUpload "${localPath}" "${hdfsPath}" ${mode}
                ;;
            2)
                read -p "hdfs path: " hdfsPath
                read -p "local path: " localPath
                read -p "auto rename if exists? (0/[1]): " rename
                if [[ ${rename} -ne 0 ]];then
                    rename=1
                else
                    rename=0
                fi
                handleDownload "${hdfsPath}" "${localPath}" ${rename}
                ;;
            3)
                read -p "hdfs path: " hdfsPath
                handleRead "${hdfsPath}"
                ;;
            4)
                read -p "hdfs path: " hdfsPath
                read -p "recursive? (0/[1]): " recursive
                if [[ ${recursive} -ne 0 ]];then
                    recursive=1
                else
                    recursive=0
                fi
                handleInspect "${hdfsPath}" ${recursive}
                ;;
            5)
                read -p "hdfs path: " hdfsPath
                handleCreate "${hdfsPath}"
                ;;
            6)
                read -p "hdfs path: " hdfsPath
                handleDelete "${hdfsPath}"
                ;;
            7)
                read -p "source hdfs path: " srcPath
                read -p "destination hdfs path: " destPath
                handleMove "${srcPath}" "${destPath}"
                ;;
            *)
                echo "Unknown choice: ${choice}"
                ;;
        esac
    done
}

if [[ $# -gt 0 ]];then
    handleArgs $@
else
    interactiveMode
fi
