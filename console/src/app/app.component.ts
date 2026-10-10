import { Component, OnInit, computed, inject, signal } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { DatePipe } from "@angular/common";
import { firstValueFrom } from "rxjs";

type Practice = {
  id: string;
  title: string;
  status: string;
  version: number;
  created_at: string;
};
type Event = {
  eventId: string;
  sequence: number;
  type: string;
  occurredAt: string;
  description: string;
};
type View = {
  id: string;
  title: string;
  status: string;
  lastSequence: number;
  documents: string[];
  events: Event[];
  pending: unknown[];
};
type Health = {
  consumerPaused: boolean;
  pendingOutbox: number;
  duplicateDeliveries: number;
};

@Component({
  selector: "app-root",
  standalone: true,
  imports: [DatePipe],
  templateUrl: "./app.component.html",
})
export class AppComponent implements OnInit {
  private readonly http = inject(HttpClient);
  private readonly api = "http://localhost:8092/api";
  readonly practices = signal<Practice[]>([]);
  readonly views = signal<View[]>([]);
  readonly health = signal<Health>({
    consumerPaused: false,
    pendingOutbox: 0,
    duplicateDeliveries: 0,
  });
  readonly selectedId = signal<string | null>(null);
  readonly title = signal("");
  readonly busy = signal(false);
  readonly error = signal("");
  readonly notice = signal("");
  readonly selected = computed(() =>
    this.practices().find((p) => p.id === this.selectedId()),
  );
  readonly approvedCount = computed(
    () => this.practices().filter((p) => p.status === "APPROVED").length,
  );
  readonly projection = computed(() =>
    this.views().find((v) => v.id === this.selectedId()),
  );
  readonly lag = computed(() =>
    Math.max(
      0,
      (this.selected()?.version ?? 0) - (this.projection()?.lastSequence ?? 0),
    ),
  );

  ngOnInit(): void {
    void this.refresh();
    window.setInterval(() => void this.refresh(true), 1500);
  }

  async refresh(silent = false): Promise<void> {
    try {
      const [practices, views, health] = await Promise.all([
        firstValueFrom(this.http.get<Practice[]>(`${this.api}/practices`)),
        firstValueFrom(this.http.get<View[]>(`${this.api}/dashboard`)),
        firstValueFrom(this.http.get<Health>(`${this.api}/dashboard/health`)),
      ]);
      this.practices.set(practices);
      this.views.set(views);
      this.health.set(health);
      if (!this.selectedId() && practices.length)
        this.selectedId.set(practices[0].id);
      if (!silent) this.error.set("");
    } catch {
      if (!silent)
        this.error.set(
          "Could not reach the API. Check that the stack is running.",
        );
    }
  }

  async create(): Promise<void> {
    const title = this.title().trim();
    if (!title) return;
    await this.run(async () => {
      const response = await firstValueFrom(
        this.http.post<{ id: string }>(`${this.api}/practices`, { title }),
      );
      this.selectedId.set(response.id);
      this.title.set("");
      this.notice.set(
        "Practice committed to PostgreSQL. The dashboard will catch up through Kafka.",
      );
    });
  }

  async upload(input: HTMLInputElement): Promise<void> {
    const id = this.selectedId();
    const file = input.files?.[0];
    if (!id || !file) return;
    await this.run(async () => {
      const body = new FormData();
      body.append("file", file);
      await firstValueFrom(
        this.http.post(`${this.api}/practices/${id}/documents`, body),
      );
      input.value = "";
      this.notice.set(
        "Document and event committed. Projection update is pending.",
      );
    });
  }

  async approve(): Promise<void> {
    const id = this.selectedId();
    if (!id) return;
    await this.run(async () => {
      await firstValueFrom(
        this.http.post(`${this.api}/practices/${id}/approve`, {}),
      );
      this.notice.set(
        "Approval committed. Watch the dashboard version catch up.",
      );
    });
  }

  async control(
    action: "pause" | "resume" | "fail-next" | "rebuild",
  ): Promise<void> {
    await this.run(async () => {
      const result = await firstValueFrom(
        this.http.post<Record<string, unknown>>(
          `${this.api}/dashboard/${action}`,
          {},
        ),
      );
      this.notice.set(
        action === "rebuild"
          ? `Replayed ${result["replayedEvents"]} Kafka events into MongoDB.`
          : `Consumer action: ${action}.`,
      );
    });
  }

  async duplicateLast(): Promise<void> {
    const id = this.selectedId();
    if (!id) return;
    await this.run(async () => {
      await firstValueFrom(
        this.http.post(`${this.api}/dashboard/duplicate-last/${id}`, {}),
      );
      this.notice.set(
        "Last event sent again. The projection will ignore its applied sequence.",
      );
    });
  }

  viewFor(id: string): View | undefined {
    return this.views().find((view) => view.id === id);
  }

  private async run(action: () => Promise<void>): Promise<void> {
    this.busy.set(true);
    this.error.set("");
    try {
      await action();
      await this.refresh();
    } catch (error: unknown) {
      const response = error as { error?: { error?: string } };
      this.error.set(
        response.error?.error ??
          "The request failed. Check API logs for details.",
      );
    } finally {
      this.busy.set(false);
    }
  }
}
