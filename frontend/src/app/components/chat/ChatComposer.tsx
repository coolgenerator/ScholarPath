import React, { useRef, useState } from 'react';

interface ChatComposerProps {
  value: string;
  placeholder: string;
  onChange: (value: string) => void;
  onSend: () => void;
  onKeyDown: (event: React.KeyboardEvent) => void;
}

interface AttachedFile {
  file: File;
  preview?: string;
}

export function ChatComposer({
  value,
  placeholder,
  onChange,
  onSend,
  onKeyDown,
}: ChatComposerProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [attachments, setAttachments] = useState<AttachedFile[]>([]);

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (!files) return;
    const newAttachments: AttachedFile[] = [];
    for (const file of Array.from(files)) {
      const preview = file.type.startsWith('image/') ? URL.createObjectURL(file) : undefined;
      newAttachments.push({ file, preview });
    }
    setAttachments((prev) => [...prev, ...newAttachments]);
    e.target.value = '';
  };

  const removeAttachment = (index: number) => {
    setAttachments((prev) => {
      const removed = prev[index];
      if (removed.preview) URL.revokeObjectURL(removed.preview);
      return prev.filter((_, i) => i !== index);
    });
  };

  const formatFileSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes}B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
  };

  const fileIcon = (type: string) => {
    if (type.startsWith('image/')) return 'image';
    if (type === 'application/pdf') return 'picture_as_pdf';
    if (type.includes('spreadsheet') || type.includes('csv') || type.includes('excel')) return 'table_chart';
    return 'description';
  };

  return (
    <div className="sticky bottom-0 z-20 bg-gradient-to-t from-white via-white/96 to-white/0 px-4 pb-4 pt-4 sm:px-5 sm:pb-6 lg:px-6">
      <div className="w-full">
        {/* Attachment previews */}
        {attachments.length > 0 && (
          <div className="mb-2 flex flex-wrap gap-2 px-1">
            {attachments.map((att, i) => (
              <div
                key={`${att.file.name}-${i}`}
                className="group relative flex items-center gap-2 rounded-xl border border-outline-variant/15 bg-white px-3 py-2 text-xs shadow-sm"
              >
                {att.preview ? (
                  <img src={att.preview} alt="" className="h-8 w-8 rounded-lg object-cover" />
                ) : (
                  <span className="material-symbols-outlined text-base text-on-surface-variant/60">{fileIcon(att.file.type)}</span>
                )}
                <div className="max-w-[120px]">
                  <div className="truncate font-semibold text-on-surface">{att.file.name}</div>
                  <div className="text-[10px] text-on-surface-variant/50">{formatFileSize(att.file.size)}</div>
                </div>
                <button
                  onClick={() => removeAttachment(i)}
                  className="absolute -right-1.5 -top-1.5 flex h-5 w-5 items-center justify-center rounded-full bg-surface-container-high text-on-surface-variant opacity-0 shadow-sm transition group-hover:opacity-100"
                >
                  <span className="material-symbols-outlined text-xs">close</span>
                </button>
              </div>
            ))}
          </div>
        )}

        <div className="relative flex items-center rounded-[1.75rem] border border-outline-variant/15 bg-white/92 px-4 py-3 shadow-[0_22px_52px_rgba(15,23,42,0.12)] backdrop-blur transition-all duration-300 focus-within:-translate-y-0.5 focus-within:border-primary/20 focus-within:shadow-[0_26px_64px_rgba(0,64,161,0.14)] sm:px-5 sm:py-4">
          <div className="pointer-events-none absolute inset-x-8 top-0 h-px bg-gradient-to-r from-transparent via-primary/25 to-transparent"></div>

          <input
            ref={fileInputRef}
            type="file"
            multiple
            accept="image/*,.pdf,.csv,.xlsx,.xls,.doc,.docx,.txt"
            className="hidden"
            onChange={handleFileSelect}
          />

          <button
            onClick={() => fileInputRef.current?.click()}
            className="mr-2 flex h-9 w-9 shrink-0 items-center justify-center rounded-xl text-on-surface-variant/45 transition-colors hover:bg-surface-container-high/50 hover:text-on-surface-variant/80"
            title="上传文件"
          >
            <span className="material-symbols-outlined text-[20px]">attach_file</span>
          </button>

          <input
            className="flex-1 border-none bg-transparent py-1 text-sm text-on-surface placeholder:text-on-surface-variant/55 outline-none focus:ring-0"
            placeholder={placeholder}
            type="text"
            value={value}
            onChange={(event) => onChange(event.target.value)}
            onKeyDown={onKeyDown}
          />
          <button
            onClick={onSend}
            disabled={!value.trim() && attachments.length === 0}
            className="ml-3 flex h-11 w-11 items-center justify-center rounded-2xl bg-primary text-on-primary shadow-[0_16px_34px_rgba(3,2,19,0.22)] transition-all duration-300 hover:-translate-y-0.5 hover:scale-[1.03] disabled:opacity-50 disabled:hover:translate-y-0 disabled:hover:scale-100"
          >
            <span className="material-symbols-outlined text-sm font-bold">arrow_upward</span>
          </button>
        </div>
      </div>
    </div>
  );
}
