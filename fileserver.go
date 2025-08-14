package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	logger = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds|log.Lmsgprefix)
)

type GetFileInfo struct {
	StartTimeNs   int64          `json:"time_start_ns"`
	EndTimeNs     int64          `json:"time_end_ns"`
	Duration      string         `json:"time_duration"`   // human-readable duration between start and end
	FileSize      uint64         `json:"file_size"`       // size of the file for this convert_id
	FileSizeHuman string         `json:"file_size_human"` // human-readable file size
	Ranges        []RangeRequest `json:"ranges"`
}

type RangeRequest struct {
	StartByte          uint64  `json:"byte_start"`
	EndByte            int64   `json:"byte_end"` // -1 if open-ended / hit EOF
	RequestLen         uint64  `json:"request_len"`
	RequestLenHuman    string  `json:"request_len_human"`
	ReturnedBytes      uint64  `json:"returned_bytes"`
	ReturnedBytesHuman string  `json:"returned_bytes_human"`
	PercReturned       float64 `json:"returned_perc"` // percentage of returned_bytes / request_len
	StartTimeNs        int64   `json:"time_start_ns"`
	EndTimeNs          int64   `json:"time_end_ns"`
	Duration           string  `json:"time_duration"` // human-readable duration for this range
	ClientCancelled    bool    `json:"client_cancelled"`
	RemoteAddr         string  `json:"client_addr"`
	EndUnspecified     bool    `json:"-"` // internal: was end unspecified in request
}

var (
	infoMu  sync.Mutex
	infoMap = make(map[string]*GetFileInfo)
)

func main() {
	http.HandleFunc("/getfile/", getFileHandler)
	http.HandleFunc("/getinfo", getInfoHandler)

	addr := ":7777"
	log.Printf("Starting server on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func humanReadableBytes(b uint64) string {
	size := float64(b)
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	exp := 0
	for size >= 1024 && exp < len(units)-1 {
		size /= 1024
		exp++
	}
	if exp == 0 {
		return fmt.Sprintf("%.0f %s", size, units[exp])
	}
	return fmt.Sprintf("%.1f %s", size, units[exp])
}

func getFileHandler(w http.ResponseWriter, r *http.Request) {
	prefix0 := fmt.Sprintf("raddr=%s", r.RemoteAddr)
	lg := log.New(os.Stderr, prefix0, log.LstdFlags|log.Lmicroseconds|log.Lmsgprefix)
	path := fmt.Sprintf(" path=%s ", r.URL.Path)
	lg.Printf(" [START] accept request")

	rawPath := strings.TrimPrefix(r.URL.Path, "/getfile")
	if rawPath == "" {
		lg.Printf("%smissing filepath", path)
		http.Error(w, "missing filepath", http.StatusBadRequest)
		return
	}
	cleanPath := filepath.Clean(rawPath)
	if strings.Contains(cleanPath, "..") {
		lg.Printf("%sinvalid filepath %s", path, cleanPath)
		http.Error(w, "invalid filepath", http.StatusBadRequest)
		return
	}

	convertID := r.URL.Query().Get("convert_id")
	if convertID == "" {
		lg.Printf("%smissing convert_id", path)
		http.Error(w, "missing convert_id", http.StatusBadRequest)
		return
	}

	// enrich prefix with convert_id
	prefix1 := fmt.Sprintf("%s,cid=%s", prefix0, convertID)
	lg = log.New(os.Stderr, prefix1, log.LstdFlags|log.Lmicroseconds|log.Lmsgprefix)

	f, err := os.Open(cleanPath)
	if err != nil {
		lg.Printf("cannot open file: %v", err)
		http.Error(w, "cannot open file: "+err.Error(), http.StatusNotFound)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		lg.Printf("stat error: %v", err)
		http.Error(w, "stat error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if fi.IsDir() {
		lg.Printf("path is directory")
		http.Error(w, "path is directory", http.StatusBadRequest)
		return
	}
	fileSize := fi.Size()

	start, end, isPartial, endUnspecified, err := parseRangeHeader(r.Header.Get("Range"), fileSize)
	if err != nil {
		lg.Printf("bad range header: %v", err)
		http.Error(w, "bad range header: "+err.Error(), http.StatusBadRequest)
		return
	}
	if end >= uint64(fileSize) {
		end = uint64(fileSize - 1)
	}
	if start > end {
		lg.Printf("invalid range start=%d end=%d filesize=%d", start, end, fileSize)
		http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	prefix2 := fmt.Sprintf("%s,s=%d,e=%d ", prefix1, start, end)
	lg = log.New(os.Stderr, prefix2, log.LstdFlags|log.Lmicroseconds|log.Lmsgprefix)

	expectedLen := end - start + 1

	// Determine stored endByte: -1 if unspecified or hit EOF (end+1 == fileSize)
	var endByteForRecord int64
	if endUnspecified || end+1 == uint64(fileSize) {
		endByteForRecord = -1
	} else {
		endByteForRecord = int64(end)
	}

	// Record request info
	infoMu.Lock()
	gfi, ok := infoMap[convertID]
	if !ok {
		gfi = &GetFileInfo{}
		infoMap[convertID] = gfi
	}
	if gfi.FileSize == 0 {
		gfi.FileSize = uint64(fileSize)
		gfi.FileSizeHuman = humanReadableBytes(uint64(fileSize))
	}
	startTimeNs := time.Now().UnixNano()
	if gfi.StartTimeNs == 0 {
		gfi.StartTimeNs = startTimeNs
	}
	rreq := RangeRequest{
		StartByte:       start,
		EndByte:         endByteForRecord,
		RequestLen:      expectedLen,
		RequestLenHuman: humanReadableBytes(expectedLen),
		StartTimeNs:     startTimeNs,
		RemoteAddr:      r.RemoteAddr,
		EndUnspecified:  endUnspecified,
	}
	gfi.Ranges = append(gfi.Ranges, rreq)
	rangeIndex := len(gfi.Ranges) - 1
	infoMu.Unlock()

	_, err = f.Seek(int64(start), io.SeekStart)
	if err != nil {
		lg.Printf("seek error: %v", err)
		http.Error(w, "seek error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatUint(expectedLen, 10))
	if isPartial {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	reader := io.LimitReader(f, int64(expectedLen))
	bytesCopied, copyErr := io.Copy(w, reader)

	clientCancelled := r.Context().Err() != nil

	if int64(expectedLen) == bytesCopied {
		lg.Printf("[END: ok] expected bytes to return=%d, actual copied=%d", expectedLen, bytesCopied)
	} else {
		lg.Printf("[END: partial/cancel] expected bytes to return=%d, actual copied=%d", expectedLen, bytesCopied)
	}

	if clientCancelled {
		lg.Printf("client cancelled")
	}

	if copyErr != nil {
		lg.Printf("copy error: %v", copyErr)
	}

	endTime := time.Now().UnixNano()

	// Update info map with end times, returned bytes, durations, percentages.
	infoMu.Lock()
	if gfi2, ok2 := infoMap[convertID]; ok2 {
		if rangeIndex < len(gfi2.Ranges) {
			gfi2.Ranges[rangeIndex].ReturnedBytes = uint64(bytesCopied)
			gfi2.Ranges[rangeIndex].ReturnedBytesHuman = humanReadableBytes(uint64(bytesCopied))
			if gfi2.Ranges[rangeIndex].RequestLen > 0 {
				gfi2.Ranges[rangeIndex].PercReturned = float64(gfi2.Ranges[rangeIndex].ReturnedBytes) / float64(gfi2.Ranges[rangeIndex].RequestLen) * 100.0
			}
			gfi2.Ranges[rangeIndex].EndTimeNs = endTime
			gfi2.Ranges[rangeIndex].ClientCancelled = clientCancelled
			// time_duration for this range
			durRange := time.Duration(gfi2.Ranges[rangeIndex].EndTimeNs - gfi2.Ranges[rangeIndex].StartTimeNs)
			gfi2.Ranges[rangeIndex].Duration = durRange.String()

			// update parent time_end_ns and time_duration
			if endTime > gfi2.EndTimeNs {
				gfi2.EndTimeNs = endTime
			}
			if gfi2.EndTimeNs > gfi2.StartTimeNs {
				gfi2.Duration = time.Duration(gfi2.EndTimeNs - gfi2.StartTimeNs).String()
			}
		}
	}
	infoMu.Unlock()
}

// parseRangeHeader parses only the HTTP Range header.
// Accepts forms like "bytes=START-END" or "bytes=START-".
// Returns start, end, isPartial=true if a Range header was present, endUnspecified=true if the end was not provided explicitly.
func parseRangeHeader(rangeHdr string, fileSize int64) (start, end uint64, isPartial, endUnspecified bool, err error) {
	if rangeHdr == "" {
		// full file: treat end as unspecified (open-ended)
		return 0, uint64(fileSize - 1), false, true, nil
	}

	raw := strings.TrimSpace(rangeHdr)
	if !strings.HasPrefix(raw, "bytes=") {
		return 0, 0, false, false, fmt.Errorf("range must start with bytes= prefix")
	}
	raw = strings.TrimPrefix(raw, "bytes=")
	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return 0, 0, false, false, fmt.Errorf("invalid range format: %q", raw)
	}
	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])

	if startStr == "" {
		return 0, 0, false, false, fmt.Errorf("start missing in range")
	}
	start64, err := strconv.ParseUint(startStr, 10, 64)
	if err != nil {
		return 0, 0, false, false, fmt.Errorf("invalid start: %v", err)
	}
	var end64 uint64
	endUnspecified = false
	if endStr == "" {
		end64 = uint64(fileSize - 1)
		endUnspecified = true
	} else {
		end64, err = strconv.ParseUint(endStr, 10, 64)
		if err != nil {
			return 0, 0, false, false, fmt.Errorf("invalid end: %v", err)
		}
	}
	return start64, end64, true, endUnspecified, nil
}

func getInfoHandler(w http.ResponseWriter, r *http.Request) {
	convertID := r.URL.Query().Get("convert_id")
	if convertID == "" {
		http.Error(w, "missing convert_id", http.StatusBadRequest)
		return
	}

	infoMu.Lock()
	gfi, ok := infoMap[convertID]
	infoMu.Unlock()

	if !ok {
		logger.Printf("convert_id not found: %s", convertID)
		http.Error(w, "convert_id not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(gfi); err != nil {
		logger.Printf("convert_id: %s, json encode error: %v", convertID, err)
		http.Error(w, "json encode error: "+err.Error(), http.StatusInternalServerError)
	}
}
