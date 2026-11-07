package deploy

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var domesticRegions = []string{"ap-beijing", "ap-chengdu", "ap-guangzhou", "ap-shanghai", "ap-nanjing", "ap-hongkong"}
var foreignRegions = []string{"ap-singapore", "ap-bangkok", "ap-seoul", "ap-tokyo", "eu-frankfurt", "na-ashburn", "na-siliconvalley"}
var regionPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)+$`)
var namePattern = regexp.MustCompile(`^[A-Za-z](?:[A-Za-z0-9_-]*[A-Za-z0-9])?$`)

// ResolveRegions 展开地域组或显式地域列表，并保留首次出现的顺序。
func ResolveRegions(regions, area string) ([]string, error) {
	if (strings.TrimSpace(regions) == "") == (area == "") {
		return nil, errors.New("必须且只能指定 -r 地域列表或 -a 地域组")
	}
	var selected []string
	if area != "" {
		switch area {
		case "domestic":
			selected = domesticRegions
		case "foreign":
			selected = foreignRegions
		case "all":
			selected = append(append([]string(nil), domesticRegions...), foreignRegions...)
		default:
			return nil, errors.New("地域组必须是 domestic、foreign 或 all")
		}
	} else {
		selected = strings.Split(regions, ",")
	}
	result := make([]string, 0, len(selected))
	seen := make(map[string]bool)
	for _, r := range selected {
		r = strings.TrimSpace(r)
		if !regionPattern.MatchString(r) {
			return nil, errors.New("地域名格式无效")
		}
		if !seen[r] {
			result = append(result, r)
			seen[r] = true
		}
	}
	return result, nil
}

// ValidatePrefix 校验名称前缀；完整函数名还需通过 FunctionName 的长度校验。
func ValidatePrefix(prefix string) error {
	if len(prefix) < 1 || len(prefix) > 60 || !namePattern.MatchString(prefix) {
		return errors.New("prefix 必须以字母开头，仅含字母数字、-、_，结尾为字母数字，长度不超过 60")
	}
	return nil
}
func validFunctionName(name string) bool {
	return len(name) >= 2 && len(name) <= 60 && namePattern.MatchString(name)
}
func FunctionName(prefix, region string, index int) (string, error) {
	if err := ValidatePrefix(prefix); err != nil {
		return "", err
	}
	if index < 1 || !regionPattern.MatchString(region) {
		return "", errors.New("地域格式无效或函数索引不是正数")
	}
	name := prefix + "-" + region + "-" + strconv.Itoa(index)
	if !validFunctionName(name) {
		return "", errors.New("完整函数名必须为 2-60 个字符，请缩短 prefix")
	}
	return name, nil
}

// Endpoint 是可写入客户端地址文件的非敏感部署结果。
type Endpoint struct{ Region, Function, URL string }

func validEndpoint(value string) bool {
	u, err := url.Parse(value)
	return err == nil && (u.Scheme == "ws" || u.Scheme == "wss") && u.Host != "" && u.Hostname() != "" && u.User == nil && u.Fragment == ""
}
func endpointLines(data []byte) []string {
	lines := strings.Split(string(data), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}
func parseEndpoints(lines []string) ([]string, error) {
	var result []string
	seen := make(map[string]bool)
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !validEndpoint(line) {
			return nil, fmt.Errorf("endpoints 文件第 %d 行不是有效的 ws/wss 地址", i+1)
		}
		if !seen[line] {
			result = append(result, line)
			seen[line] = true
		}
	}
	return result, nil
}
func ReadEndpoints(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseEndpoints(endpointLines(data))
}

func generatedComment(line string) bool {
	fields := strings.Fields(line)
	return len(fields) == 3 && fields[0] == "#" && regionPattern.MatchString(fields[1]) && validFunctionName(fields[2])
}

// writeEndpointLines 使用同目录临时文件替换，避免中途失败破坏原有文件。
func writeEndpointLines(path string, lines []string) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".scfproxy-endpoints-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(0644); err != nil {
		f.Close()
		return err
	}
	data := strings.Join(lines, "\n")
	if len(lines) > 0 {
		data += "\n"
	}
	if _, err = f.WriteString(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(temp, path)
}
func MergeEndpoints(path string, add []Endpoint) error {
	for _, e := range add {
		if !validEndpoint(e.URL) || !regionPattern.MatchString(e.Region) || !validFunctionName(e.Function) {
			return errors.New("部署 endpoint 或其地域/函数名无效")
		}
	}
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	lines := endpointLines(data)
	if _, err = parseEndpoints(lines); err != nil {
		return err
	}
	seen := make(map[string]bool)
	kept := make([]string, 0, len(lines)+2*len(add))
	for _, line := range lines {
		value := strings.TrimSpace(line)
		if value != "" && !strings.HasPrefix(value, "#") {
			if seen[value] {
				if len(kept) > 0 && generatedComment(kept[len(kept)-1]) {
					kept = kept[:len(kept)-1]
				}
				continue
			}
			seen[value] = true
		}
		kept = append(kept, line)
	}
	for _, e := range add {
		if seen[e.URL] {
			continue
		}
		kept = append(kept, "# "+e.Region+" "+e.Function, e.URL)
		seen[e.URL] = true
	}
	return writeEndpointLines(path, kept)
}
func RemoveEndpoints(path string, urls []string) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	remove := make(map[string]bool, len(urls))
	for _, u := range urls {
		remove[u] = true
	}
	lines := endpointLines(data)
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if remove[strings.TrimSpace(line)] {
			if len(kept) > 0 && generatedComment(kept[len(kept)-1]) {
				kept = kept[:len(kept)-1]
			}
			continue
		}
		kept = append(kept, line)
	}
	return writeEndpointLines(path, kept)
}
